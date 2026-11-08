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
    pub session_tag: Option<String>,
    #[allow(dead_code)]
    pub query_pattern: String,
    pub compiled_regex: Regex,
    pub with_params: Option<Vec<serde_json::Value>>,
    pub action: BarrierAction,
    pub times: Option<u32>,
}

#[derive(Deserialize)]
pub struct InterceptRuleInput {
    #[serde(alias = "clientPid", alias = "client_pid", alias = "targetPid")]
    pub target_pid: Option<u32>,
    #[serde(alias = "sessionTag", alias = "session_tag", alias = "session")]
    pub session_tag: Option<String>,
    #[serde(alias = "queryPattern", alias = "onQueryMatch")]
    pub query_pattern: String,
    #[serde(alias = "withParams", alias = "with_params", default)]
    pub with_params: Option<Vec<serde_json::Value>>,
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
            session_tag: input.session_tag,
            query_pattern: input.query_pattern,
            compiled_regex,
            with_params: input.with_params,
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

pub fn extract_session_tag(query: &str) -> Option<String> {
    if let Some(start) = query.find("/*")
        && let Some(end) = query[start..].find("*/")
    {
        let comment = &query[start + 2..start + end];
        let trimmed = comment.trim();
        if let Some(rest) = trimmed.strip_prefix("interleave:") {
            for token in rest.split([',', ' ', ';']) {
                let token = token.trim();
                if let Some(tag) = token
                    .strip_prefix("session=")
                    .or_else(|| token.strip_prefix("tag="))
                {
                    return Some(tag.trim_matches(['\'', '"']).to_string());
                } else if !token.is_empty() && !token.contains('=') {
                    return Some(token.trim_matches(['\'', '"']).to_string());
                }
            }
        }
    }
    None
}

pub fn matches_params(expected: Option<&[serde_json::Value]>, actual: &[Option<String>]) -> bool {
    let Some(expected) = expected else {
        return true;
    };

    if expected.len() != actual.len() {
        return false;
    }

    for (exp, act) in expected.iter().zip(actual.iter()) {
        match (exp, act) {
            (serde_json::Value::String(s), _) if s == "*" => continue,
            (serde_json::Value::Null, None) => continue,
            (serde_json::Value::String(s), Some(a)) if s == a => continue,
            (serde_json::Value::Bool(b), Some(a)) => {
                let short = if *b { "t" } else { "f" };
                if *a == b.to_string() || *a == short {
                    continue;
                }
                return false;
            }
            _ => return false,
        }
    }

    true
}

#[derive(Clone, Copy, Debug)]
pub struct Fuzzer {
    pub base_seed: u64,
    pub max_delay_ms: u64,
}

impl Fuzzer {
    pub fn new(base_seed: u64, max_delay_ms: u64) -> Self {
        Self {
            base_seed,
            max_delay_ms,
        }
    }

    pub fn for_connection(&self, conn_id: u64) -> ConnFuzzer {
        let mut mixed = self.base_seed.wrapping_add(0x9e3779b97f4a7c15);
        mixed ^= conn_id.wrapping_mul(0xbf58476d1ce4e5b9);
        mixed = (mixed ^ (mixed >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
        mixed = (mixed ^ (mixed >> 27)).wrapping_mul(0x94d049bb133111eb);
        let conn_seed = mixed ^ (mixed >> 31);

        ConnFuzzer {
            rng: fastrand::Rng::with_seed(conn_seed),
            max_delay_ms: self.max_delay_ms,
        }
    }
}

pub struct ConnFuzzer {
    rng: fastrand::Rng,
    max_delay_ms: u64,
}

impl ConnFuzzer {
    pub fn next_delay(&mut self) -> u64 {
        if self.max_delay_ms == 0 {
            0
        } else {
            self.rng.u64(0..=self.max_delay_ms)
        }
    }
}

pub struct ActiveBarrier {
    #[allow(dead_code)]
    pub id: u64,
    #[allow(dead_code)]
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
    pub fuzzer: Option<Fuzzer>,
}

#[derive(Clone)]
pub struct SharedState {
    pub inner: Arc<Mutex<BarrierState>>,
    pub barrier_counter: Arc<AtomicU64>,
}

impl SharedState {
    pub fn new(fuzz_seed: Option<u64>, fuzz_max_delay_ms: u64) -> Self {
        let fuzzer = fuzz_seed.map(|seed| Fuzzer::new(seed, fuzz_max_delay_ms));
        let state = BarrierState {
            fuzzer,
            ..BarrierState::default()
        };
        Self {
            inner: Arc::new(Mutex::new(state)),
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

    pub async fn check_deadlock_match(
        &self,
        pid: Option<u32>,
        session_tag: Option<&str>,
        query: &str,
        params: &[Option<String>],
    ) -> bool {
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
            if let Some(ref rule_tag) = rule.session_tag
                && Some(rule_tag.as_str()) != session_tag
            {
                continue;
            }
            if !matches_params(rule.with_params.as_deref(), params) {
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
        session_tag: Option<&str>,
        query: &str,
        params: &[Option<String>],
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
            if let Some(ref rule_tag) = rule.session_tag
                && Some(rule_tag.as_str()) != session_tag
            {
                continue;
            }
            if !matches_params(rule.with_params.as_deref(), params) {
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
        session_tag: Option<&str>,
        query: &str,
        action: BarrierAction,
        params: &[Option<String>],
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
            if let Some(ref rule_tag) = rule.session_tag
                && Some(rule_tag.as_str()) != session_tag
            {
                continue;
            }
            if !matches_params(rule.with_params.as_deref(), params) {
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
            session_tag: None,
            query_pattern: "manual".to_string(),
            compiled_regex: Regex::new(".*").unwrap(),
            with_params: None,
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

    pub async fn should_drop_on_commit(
        &self,
        pid: Option<u32>,
        session_tag: Option<&str>,
        commit_tag: &str,
    ) -> bool {
        let mut state = self.inner.lock().await;
        let mut should_drop = false;

        state.rules.retain_mut(|rule| {
            if rule.action == BarrierAction::DropConnectionOnCommit {
                if let Some(target_pid) = rule.target_pid
                    && Some(target_pid) != pid
                {
                    return true;
                }
                if let Some(ref rule_tag) = rule.session_tag
                    && Some(rule_tag.as_str()) != session_tag
                {
                    return true;
                }
                if rule.compiled_regex.is_match(commit_tag) {
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

    pub async fn cleanup_connection(&self, conn_id: u64, pid: Option<u32>) {
        let mut st = self.inner.lock().await;
        st.client_statements.remove(&conn_id);
        if let Some(p) = pid {
            st.pid_to_conn.remove(&p);
        }
    }

    pub async fn fuzzer(&self) -> Option<Fuzzer> {
        let state = self.inner.lock().await;
        state.fuzzer
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fuzzer_deterministic_per_connection_sequence() {
        let fuzzer = Fuzzer::new(42, 50);

        let mut conn1_a = fuzzer.for_connection(1);
        let mut conn1_b = fuzzer.for_connection(1);
        let mut conn2 = fuzzer.for_connection(2);

        let seq1_a: Vec<u64> = (0..5).map(|_| conn1_a.next_delay()).collect();
        let seq1_b: Vec<u64> = (0..5).map(|_| conn1_b.next_delay()).collect();
        let seq2: Vec<u64> = (0..5).map(|_| conn2.next_delay()).collect();

        assert_eq!(seq1_a, seq1_b);
        assert_ne!(seq1_a, seq2);
    }
}
