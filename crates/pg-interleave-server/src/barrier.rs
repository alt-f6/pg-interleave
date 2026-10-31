use regex::Regex;
use serde::{Deserialize, Deserializer, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::{Mutex, oneshot};
use tracing::info;

#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum BarrierAction {
    #[serde(alias = "PauseBeforeQuery", alias = "PAUSE_BEFORE_QUERY")]
    PauseBeforeQuery,
    #[serde(alias = "PauseAfterResponse", alias = "PAUSE_AFTER_RESPONSE")]
    PauseAfterResponse,
    #[serde(alias = "DropConnectionOnCommit", alias = "DROP_CONNECTION_ON_COMMIT")]
    DropConnectionOnCommit,
    #[serde(alias = "SyntheticDeadlock", alias = "SYNTHETIC_DEADLOCK")]
    SyntheticDeadlock,
}

#[derive(Debug, Clone)]
pub struct InterceptRule {
    pub target_pid: Option<u32>,
    #[allow(dead_code)]
    pub query_pattern: String,
    pub compiled_regex: Regex,
    pub action: BarrierAction,
    pub times: Option<u32>,
}

#[derive(Deserialize)]
pub struct InterceptRuleInput {
    #[serde(alias = "clientPid", alias = "client_pid", alias = "targetPid")]
    pub target_pid: Option<u32>,
    #[serde(alias = "queryPattern", alias = "onQueryMatch")]
    pub query_pattern: String,
    pub action: BarrierAction,
    #[serde(default = "default_times")]
    pub times: Option<u32>,
}

fn default_times() -> Option<u32> {
    Some(1)
}

impl TryFrom<InterceptRuleInput> for InterceptRule {
    type Error = regex::Error;

    fn try_from(input: InterceptRuleInput) -> Result<Self, Self::Error> {
        let compiled_regex = Regex::new(&input.query_pattern)?;
        Ok(Self {
            target_pid: input.target_pid,
            query_pattern: input.query_pattern,
            compiled_regex,
            action: input.action,
            times: input.times,
        })
    }
}

impl<'de> Deserialize<'de> for InterceptRule {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let input = InterceptRuleInput::deserialize(deserializer)?;
        InterceptRule::try_from(input).map_err(serde::de::Error::custom)
    }
}

#[allow(dead_code)]
pub struct ActiveBarrier {
    pub id: u64,
    pub rule: InterceptRule,
    pub resume_sender: Option<oneshot::Sender<()>>,
}

#[derive(Default)]
pub struct BarrierState {
    pub rules: Vec<InterceptRule>,
    pub active_barriers: BTreeMap<u64, ActiveBarrier>,
    pub pid_to_conn: HashMap<u32, u64>,
    pub client_statements: HashMap<u64, HashMap<String, String>>,
    pub had_timeouts: bool,
}

#[derive(Clone)]
pub struct SharedState {
    pub inner: Arc<Mutex<BarrierState>>,
    pub barrier_counter: Arc<AtomicU64>,
}

impl SharedState {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(BarrierState::default())),
            barrier_counter: Arc::new(AtomicU64::new(1)),
        }
    }

    pub async fn reset(&self) {
        let mut state = self.inner.lock().await;
        for (_, barrier) in state.active_barriers.split_off(&0) {
            if let Some(sender) = barrier.resume_sender {
                let _ = sender.send(());
            }
        }
        state.rules.clear();
        state.client_statements.clear();
        state.had_timeouts = false;
        info!("Barrier state reset successfully");
    }

    pub async fn add_rule(&self, rule: InterceptRule) {
        let mut state = self.inner.lock().await;
        state.rules.push(rule);
    }

    pub async fn register_pid(&self, pid: u32, conn_id: u64) {
        let mut state = self.inner.lock().await;
        state.pid_to_conn.insert(pid, conn_id);
    }

    pub async fn store_statement(&self, conn_id: u64, stmt_name: String, query: String) {
        let mut state = self.inner.lock().await;
        state
            .client_statements
            .entry(conn_id)
            .or_default()
            .insert(stmt_name, query);
    }

    pub async fn check_deadlock_match(&self, pid: Option<u32>, query: &str) -> bool {
        let mut state = self.inner.lock().await;
        let mut matched_index = None;

        for (idx, rule) in state.rules.iter().enumerate() {
            if rule.action != BarrierAction::SyntheticDeadlock {
                continue;
            }
            if let Some(target_pid) = rule.target_pid
                && Some(target_pid) != pid
            {
                continue;
            }
            if rule.compiled_regex.is_match(query) {
                matched_index = Some(idx);
                break;
            }
        }

        if let Some(idx) = matched_index {
            let should_remove = {
                let rule = &mut state.rules[idx];
                if let Some(ref mut count) = rule.times {
                    *count = count.saturating_sub(1);
                    *count == 0
                } else {
                    false
                }
            };
            if should_remove {
                state.rules.remove(idx);
            }
            return true;
        }

        false
    }

    pub async fn check_and_create_barrier(
        &self,
        pid: Option<u32>,
        query: &str,
    ) -> Option<(String, oneshot::Receiver<()>)> {
        let mut state = self.inner.lock().await;
        let mut matched_index = None;

        for (idx, rule) in state.rules.iter().enumerate() {
            if rule.action != BarrierAction::PauseBeforeQuery {
                continue;
            }
            if let Some(target_pid) = rule.target_pid
                && Some(target_pid) != pid
            {
                continue;
            }
            if rule.compiled_regex.is_match(query) {
                matched_index = Some(idx);
                break;
            }
        }

        if let Some(idx) = matched_index {
            let rule = state.rules[idx].clone();
            let should_remove = {
                let rule_mut = &mut state.rules[idx];
                if let Some(ref mut count) = rule_mut.times {
                    *count = count.saturating_sub(1);
                    *count == 0
                } else {
                    false
                }
            };
            if should_remove {
                state.rules.remove(idx);
            }

            let numeric_id = self.barrier_counter.fetch_add(1, Ordering::SeqCst);
            let barrier_id = format!("barrier_{}", numeric_id);
            let (tx, rx) = oneshot::channel();

            state.active_barriers.insert(
                numeric_id,
                ActiveBarrier {
                    id: numeric_id,
                    rule,
                    resume_sender: Some(tx),
                },
            );

            info!(barrier_id = %barrier_id, query = %query, "Barrier reached!");
            return Some((barrier_id, rx));
        }

        None
    }

    pub async fn check_action_match(
        &self,
        pid: Option<u32>,
        query: &str,
        action: BarrierAction,
    ) -> Option<InterceptRule> {
        let mut state = self.inner.lock().await;
        let mut matched_index = None;

        for (idx, rule) in state.rules.iter().enumerate() {
            if rule.action != action {
                continue;
            }
            if let Some(target_pid) = rule.target_pid
                && Some(target_pid) != pid
            {
                continue;
            }
            if rule.compiled_regex.is_match(query) {
                matched_index = Some(idx);
                break;
            }
        }

        if let Some(idx) = matched_index {
            let rule = state.rules[idx].clone();
            let should_remove = {
                let rule_mut = &mut state.rules[idx];
                if let Some(ref mut count) = rule_mut.times {
                    *count = count.saturating_sub(1);
                    *count == 0
                } else {
                    false
                }
            };
            if should_remove {
                state.rules.remove(idx);
            }
            return Some(rule);
        }

        None
    }

    pub async fn create_manual_barrier(&self) -> (String, oneshot::Receiver<()>) {
        let mut state = self.inner.lock().await;
        let numeric_id = self.barrier_counter.fetch_add(1, Ordering::SeqCst);
        let barrier_id = format!("barrier_{}", numeric_id);
        let (tx, rx) = oneshot::channel();

        let dummy_rule = InterceptRule {
            target_pid: None,
            query_pattern: "manual".to_string(),
            compiled_regex: Regex::new(".*").unwrap(),
            action: BarrierAction::PauseAfterResponse,
            times: Some(1),
        };

        state.active_barriers.insert(
            numeric_id,
            ActiveBarrier {
                id: numeric_id,
                rule: dummy_rule,
                resume_sender: Some(tx),
            },
        );

        (barrier_id, rx)
    }

    pub async fn should_drop_on_commit(&self, pid: Option<u32>) -> bool {
        let mut state = self.inner.lock().await;
        let mut should_drop = false;

        state.rules.retain_mut(|rule| {
            if rule.action == BarrierAction::DropConnectionOnCommit {
                if let Some(target_pid) = rule.target_pid {
                    if Some(target_pid) == pid {
                        should_drop = true;
                        if let Some(ref mut count) = rule.times {
                            *count = count.saturating_sub(1);
                            return *count > 0;
                        }
                        return false;
                    }
                } else {
                    should_drop = true;
                    if let Some(ref mut count) = rule.times {
                        *count = count.saturating_sub(1);
                        return *count > 0;
                    }
                    return false;
                }
            }
            true
        });

        should_drop
    }

    pub async fn resume_barrier(&self, barrier_str: &str) -> bool {
        let numeric_id: u64 = if let Some(stripped) = barrier_str.strip_prefix("barrier_") {
            stripped.parse().unwrap_or(0)
        } else {
            barrier_str.parse().unwrap_or(0)
        };

        let mut state = self.inner.lock().await;
        if let Some(mut barrier) = state.active_barriers.remove(&numeric_id)
            && let Some(tx) = barrier.resume_sender.take()
        {
            let _ = tx.send(());
            info!(barrier_id = %barrier_str, "Barrier released successfully");
            return true;
        }
        false
    }

    pub async fn resume_first(&self) -> Option<String> {
        let mut state = self.inner.lock().await;
        let first_id = *state.active_barriers.first_key_value()?.0;
        if let Some(mut barrier) = state.active_barriers.remove(&first_id)
            && let Some(tx) = barrier.resume_sender.take()
        {
            let _ = tx.send(());
            let barrier_id = format!("barrier_{}", first_id);
            info!(barrier_id = %barrier_id, "First active barrier in queue released successfully");
            return Some(barrier_id);
        }
        None
    }

    pub async fn record_timeout(&self, barrier_str: &str) {
        let numeric_id: u64 = if let Some(stripped) = barrier_str.strip_prefix("barrier_") {
            stripped.parse().unwrap_or(0)
        } else {
            barrier_str.parse().unwrap_or(0)
        };

        let mut state = self.inner.lock().await;
        state.had_timeouts = true;
        state.active_barriers.remove(&numeric_id);
    }

    pub async fn has_timeouts(&self) -> bool {
        let state = self.inner.lock().await;
        state.had_timeouts
    }

    pub async fn active_barrier_ids(&self) -> Vec<String> {
        let state = self.inner.lock().await;
        state
            .active_barriers
            .keys()
            .map(|id| format!("barrier_{}", id))
            .collect()
    }
}
