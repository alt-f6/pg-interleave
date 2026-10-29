use regex::Regex;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{Mutex, oneshot};
use tracing::{info, warn};

#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
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

#[derive(Debug, Clone, Deserialize)]
pub struct InterceptRule {
    #[serde(alias = "clientPid", alias = "client_pid", alias = "targetPid")]
    pub target_pid: Option<u32>,
    #[serde(alias = "queryPattern", alias = "onQueryMatch")]
    pub query_pattern: String,
    pub action: BarrierAction,
}

#[allow(dead_code)]
pub struct ActiveBarrier {
    pub id: String,
    pub rule: InterceptRule,
    pub resume_sender: Option<oneshot::Sender<()>>,
}

#[derive(Default)]
pub struct BarrierState {
    pub rules: Vec<InterceptRule>,
    pub active_barriers: HashMap<String, ActiveBarrier>,
    pub pid_to_conn: HashMap<u32, u64>,
    /// Connection ID -> (statement_name -> SQL query)
    pub client_statements: HashMap<u64, HashMap<String, String>>,
}

#[derive(Clone)]
pub struct SharedState(pub Arc<Mutex<BarrierState>>);

impl SharedState {
    pub fn new() -> Self {
        Self(Arc::new(Mutex::new(BarrierState::default())))
    }

    pub async fn reset(&self) {
        let mut state = self.0.lock().await;
        for (_, barrier) in state.active_barriers.drain() {
            if let Some(sender) = barrier.resume_sender {
                let _ = sender.send(());
            }
        }
        state.rules.clear();
        state.client_statements.clear();
        info!("Barrier state reset successfully");
    }

    pub async fn add_rule(&self, rule: InterceptRule) {
        let mut state = self.0.lock().await;
        state.rules.push(rule);
    }

    pub async fn register_pid(&self, pid: u32, conn_id: u64) {
        let mut state = self.0.lock().await;
        state.pid_to_conn.insert(pid, conn_id);
    }

    pub async fn store_statement(&self, conn_id: u64, stmt_name: String, query: String) {
        let mut state = self.0.lock().await;
        state
            .client_statements
            .entry(conn_id)
            .or_default()
            .insert(stmt_name, query);
    }

    /// Checks if the query matches a synthetic deadlock rule
    pub async fn check_deadlock_match(&self, pid: Option<u32>, query: &str) -> bool {
        let state = self.0.lock().await;
        for rule in &state.rules {
            if rule.action != BarrierAction::SyntheticDeadlock {
                continue;
            }
            if let Some(target_pid) = rule.target_pid
                && Some(target_pid) != pid
            {
                continue;
            }
            if let Ok(re) = Regex::new(&rule.query_pattern)
                && re.is_match(query)
            {
                return true;
            }
        }
        false
    }

    /// Checks if the query matches a pause barrier rule
    pub async fn check_and_create_barrier(
        &self,
        pid: Option<u32>,
        query: &str,
    ) -> Option<(String, oneshot::Receiver<()>)> {
        let mut state = self.0.lock().await;

        let mut matched_rule: Option<InterceptRule> = None;
        for rule in &state.rules {
            if rule.action != BarrierAction::PauseBeforeQuery {
                continue;
            }
            if let Some(target_pid) = rule.target_pid
                && Some(target_pid) != pid
            {
                continue;
            }
            if let Ok(re) = Regex::new(&rule.query_pattern)
                && re.is_match(query)
            {
                matched_rule = Some(rule.clone());
                break;
            }
        }

        if let Some(rule) = matched_rule {
            let barrier_id = format!(
                "barrier_{}_{}",
                pid.unwrap_or(0),
                state.active_barriers.len() + 1
            );
            let (tx, rx) = oneshot::channel();

            state.active_barriers.insert(
                barrier_id.clone(),
                ActiveBarrier {
                    id: barrier_id.clone(),
                    rule,
                    resume_sender: Some(tx),
                },
            );

            info!(barrier_id = %barrier_id, query = %query, "Barrier reached!");
            return Some((barrier_id, rx));
        }

        None
    }

    pub async fn should_drop_on_commit(&self, pid: Option<u32>) -> bool {
        let mut state = self.0.lock().await;
        let mut should_drop = false;

        state.rules.retain(|rule| {
            if rule.action == BarrierAction::DropConnectionOnCommit {
                if let Some(target_pid) = rule.target_pid {
                    if Some(target_pid) == pid {
                        should_drop = true;
                        return false;
                    }
                } else {
                    should_drop = true;
                    return false;
                }
            }
            true
        });

        should_drop
    }

    pub async fn resume_barrier(&self, barrier_id: &str) -> bool {
        let mut state = self.0.lock().await;
        if let Some(mut barrier) = state.active_barriers.remove(barrier_id)
            && let Some(tx) = barrier.resume_sender.take()
        {
            let _ = tx.send(());
            info!(barrier_id = %barrier_id, "Barrier released successfully");
            return true;
        }
        false
    }

    pub async fn resume_first(&self) -> Option<String> {
        let mut state = self.0.lock().await;
        let first_key = state.active_barriers.keys().next().cloned()?;
        if let Some(mut barrier) = state.active_barriers.remove(&first_key)
            && let Some(tx) = barrier.resume_sender.take()
        {
            let _ = tx.send(());
            info!(barrier_id = %first_key, "First active barrier in queue released successfully");
            return Some(first_key);
        }
        None
    }

    pub async fn cleanup_barrier(&self, barrier_id: &str) {
        let mut state = self.0.lock().await;
        if state.active_barriers.remove(barrier_id).is_some() {
            warn!(barrier_id = %barrier_id, "Barrier automatically released due to timeout (TTL)");
        }
    }
}
