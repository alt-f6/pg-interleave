use crate::ast::{AstSelector, extract_ast_metadata};
use crate::trace::{TraceRecorder, TraceReplayer};
use regex::Regex;
use serde::{Deserialize, Deserializer, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuleStatusDto {
    pub id: String,
    pub fired_count: u32,
    pub times_remaining: Option<u32>,
}

#[derive(Debug, Clone)]
pub struct InterceptRule {
    pub id: String,
    pub target_pid: Option<u32>,
    pub session_tag: Option<String>,
    #[allow(dead_code)]
    pub query_pattern: Option<String>,
    pub compiled_regex: Option<Regex>,
    pub ast_selector: Option<AstSelector>,
    pub with_params: Option<Vec<serde_json::Value>>,
    pub action: BarrierAction,
    pub times: Option<u32>,
    pub fired_count: u32,
}

#[derive(Deserialize)]
pub struct InterceptRuleInput {
    #[serde(alias = "clientPid", alias = "client_pid", alias = "targetPid")]
    pub target_pid: Option<u32>,
    #[serde(alias = "sessionTag", alias = "session_tag", alias = "session")]
    pub session_tag: Option<String>,
    #[serde(alias = "queryPattern", alias = "onQueryMatch", default)]
    pub query_pattern: Option<String>,
    #[serde(default)]
    pub ast: Option<AstSelector>,
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
    type Error = String;

    fn try_from(input: InterceptRuleInput) -> Result<Self, Self::Error> {
        if input.query_pattern.is_none() && input.ast.is_none() {
            return Err("Either 'query_pattern' or 'ast' must be provided".to_string());
        }

        let compiled_regex = match &input.query_pattern {
            Some(pat) => Some(Regex::new(pat).map_err(|e| e.to_string())?),
            None => None,
        };

        Ok(Self {
            id: String::new(),
            target_pid: input.target_pid,
            session_tag: input.session_tag,
            query_pattern: input.query_pattern,
            compiled_regex,
            ast_selector: input.ast,
            with_params: input.with_params,
            action: input.action,
            times: input.times,
            fired_count: 0,
        })
    }
}

impl InterceptRule {
    pub fn matches_query(&self, query: &str) -> bool {
        if let Some(ref re) = self.compiled_regex
            && !re.is_match(query)
        {
            return false;
        }

        if let Some(ref ast_sel) = self.ast_selector {
            let matches = extract_ast_metadata(query)
                .map(|meta| meta.matches(ast_sel))
                .unwrap_or(false);
            if !matches {
                return false;
            }
        }

        true
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
            (serde_json::Value::Number(n), Some(a)) if &n.to_string() == a => continue,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActiveBarrierSummary {
    pub id: String,
    pub rule_id: String,
}

#[derive(Debug)]
pub struct ActiveBarrier {
    #[allow(dead_code)]
    pub id: String,
    pub rule_id: String,
    pub reached_at: Instant,
    pub resume_sender: oneshot::Sender<()>,
}

#[derive(Default)]
pub struct BarrierState {
    pub rules: Vec<InterceptRule>,
    pub finished_rules: Vec<RuleStatusDto>,
    pub active_barriers: BTreeMap<String, ActiveBarrier>,
    pub next_barrier_id: u64,
    pub pid_to_conn: HashMap<u32, u64>,
    pub client_statements: HashMap<u64, HashMap<String, String>>,
    pub had_timeouts: bool,
    pub fuzzer: Option<Fuzzer>,
}

#[derive(Clone)]
pub struct SharedState {
    pub inner: Arc<Mutex<BarrierState>>,
    pub barrier_counter: Arc<AtomicU64>,
    pub recorder: Option<TraceRecorder>,
    pub replayer: Option<Arc<TraceReplayer>>,
}

impl SharedState {
    pub fn new(
        fuzz_seed: Option<u64>,
        fuzz_max_delay_ms: u64,
        recorder: Option<TraceRecorder>,
        replayer: Option<Arc<TraceReplayer>>,
    ) -> Self {
        let fuzzer = fuzz_seed.map(|seed| Fuzzer::new(seed, fuzz_max_delay_ms));
        let mut state = BarrierState {
            fuzzer,
            ..BarrierState::default()
        };
        state.next_barrier_id = 1;

        Self {
            inner: Arc::new(Mutex::new(state)),
            barrier_counter: Arc::new(AtomicU64::new(1)),
            recorder,
            replayer,
        }
    }

    pub async fn record_step(&self, conn_id: u64, query: &str, params: &[Option<String>]) {
        if let Some(recorder) = &self.recorder {
            recorder.record(conn_id, query, params).await;
        }
    }

    pub async fn flush_trace(&self) -> anyhow::Result<()> {
        if let Some(recorder) = &self.recorder {
            recorder.flush().await?;
        }
        Ok(())
    }

    pub async fn wait_for_replay_turn(&self, conn_id: u64, query: &str) {
        if let Some(replayer) = &self.replayer {
            replayer.wait_for_turn(conn_id, query).await;
        }
    }

    pub fn advance_replay_step(&self) {
        if let Some(replayer) = &self.replayer {
            replayer.advance_step();
        }
    }

    pub async fn reset(&self) {
        let mut state = self.inner.lock().await;
        for (_, barrier) in std::mem::take(&mut state.active_barriers) {
            let _ = barrier.resume_sender.send(());
        }
        state.rules.clear();
        state.finished_rules.clear();
        state.client_statements.clear();
        state.had_timeouts = false;
        drop(state);

        if let Some(recorder) = &self.recorder {
            recorder.clear().await;
        }

        info!("Barrier state reset successfully");
    }

    pub async fn add_rule(&self, mut rule: InterceptRule) -> String {
        let mut state = self.inner.lock().await;
        let rule_id = format!(
            "rule_{}",
            self.barrier_counter.fetch_add(1, Ordering::SeqCst)
        );
        rule.id = rule_id.clone();
        rule.fired_count = 0;
        state.rules.push(rule);
        rule_id
    }

    pub async fn get_rules_status(&self) -> Vec<RuleStatusDto> {
        let state = self.inner.lock().await;
        let mut result: Vec<RuleStatusDto> = state
            .rules
            .iter()
            .map(|r| RuleStatusDto {
                id: r.id.clone(),
                fired_count: r.fired_count,
                times_remaining: r.times,
            })
            .collect();
        result.extend(state.finished_rules.clone());
        result
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
            if rule.matches_query(query) {
                matched_index = Some(idx);
                break;
            }
        }

        if let Some(idx) = matched_index {
            let should_remove = {
                let rule = &mut state.rules[idx];
                rule.fired_count += 1;
                if let Some(ref mut count) = rule.times {
                    *count = count.saturating_sub(1);
                    *count == 0
                } else {
                    false
                }
            };
            if should_remove {
                let removed = state.rules.remove(idx);
                state.finished_rules.push(RuleStatusDto {
                    id: removed.id,
                    times_remaining: removed.times,
                    fired_count: removed.fired_count,
                });
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
            if rule.matches_query(query) {
                matched_index = Some(idx);
                break;
            }
        }

        if let Some(idx) = matched_index {
            let should_remove = {
                let rule_mut = &mut state.rules[idx];
                rule_mut.fired_count += 1;
                if let Some(ref mut count) = rule_mut.times {
                    *count = count.saturating_sub(1);
                    *count == 0
                } else {
                    false
                }
            };
            let rule = state.rules[idx].clone();
            if should_remove {
                let removed = state.rules.remove(idx);
                state.finished_rules.push(RuleStatusDto {
                    id: removed.id,
                    times_remaining: removed.times,
                    fired_count: removed.fired_count,
                });
            }

            let barrier_id = format!("barrier_{}", state.next_barrier_id);
            state.next_barrier_id += 1;
            let (tx, rx) = oneshot::channel();

            let active = ActiveBarrier {
                id: barrier_id.clone(),
                rule_id: rule.id.clone(),
                reached_at: Instant::now(),
                resume_sender: tx,
            };

            state.active_barriers.insert(barrier_id.clone(), active);

            info!(barrier_id = %barrier_id, rule_id = %rule.id, query = %query, "Barrier reached!");
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
            if rule.matches_query(query) {
                matched_index = Some(idx);
                break;
            }
        }

        if let Some(idx) = matched_index {
            let should_remove = {
                let rule_mut = &mut state.rules[idx];
                rule_mut.fired_count += 1;
                if let Some(ref mut count) = rule_mut.times {
                    *count = count.saturating_sub(1);
                    *count == 0
                } else {
                    false
                }
            };
            let rule = state.rules[idx].clone();
            if should_remove {
                let removed = state.rules.remove(idx);
                state.finished_rules.push(RuleStatusDto {
                    id: removed.id,
                    times_remaining: removed.times,
                    fired_count: removed.fired_count,
                });
            }
            return Some(rule);
        }

        None
    }

    #[allow(dead_code)]
    pub async fn create_manual_barrier(&self) -> (String, oneshot::Receiver<()>) {
        self.create_manual_barrier_with_rule(None).await
    }

    pub async fn create_manual_barrier_with_rule(
        &self,
        rule_id: Option<String>,
    ) -> (String, oneshot::Receiver<()>) {
        let mut state = self.inner.lock().await;
        let barrier_id = format!("barrier_{}", state.next_barrier_id);
        state.next_barrier_id += 1;
        let (tx, rx) = oneshot::channel();

        let active_barrier = ActiveBarrier {
            id: barrier_id.clone(),
            rule_id: rule_id.unwrap_or_else(|| format!("rule_manual_{}", barrier_id)),
            reached_at: Instant::now(),
            resume_sender: tx,
        };

        state
            .active_barriers
            .insert(barrier_id.clone(), active_barrier);

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
        let mut finished = Vec::new();

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
                if rule.matches_query(commit_tag) {
                    should_drop = true;
                    rule.fired_count += 1;
                    if let Some(ref mut count) = rule.times {
                        *count = count.saturating_sub(1);
                        if *count == 0 {
                            finished.push(RuleStatusDto {
                                id: rule.id.clone(),
                                times_remaining: rule.times,
                                fired_count: rule.fired_count,
                            });
                            return false;
                        }
                        return true;
                    }
                    finished.push(RuleStatusDto {
                        id: rule.id.clone(),
                        times_remaining: rule.times,
                        fired_count: rule.fired_count,
                    });
                    return false;
                }
            }
            true
        });

        state.finished_rules.extend(finished);
        should_drop
    }

    pub async fn resume_barrier(&self, id_or_rule_id: &str) -> bool {
        let mut state = self.inner.lock().await;
        if let Some(barrier) = state.active_barriers.remove(id_or_rule_id) {
            let _ = barrier.resume_sender.send(());
            info!(barrier_id = %id_or_rule_id, "Barrier released successfully");
            return true;
        }

        let key_to_remove = state
            .active_barriers
            .iter()
            .filter(|(_, b)| b.rule_id == id_or_rule_id)
            .min_by_key(|(_, b)| b.reached_at)
            .map(|(k, _)| k.clone());

        if let Some(key) = key_to_remove
            && let Some(barrier) = state.active_barriers.remove(&key)
        {
            let _ = barrier.resume_sender.send(());
            info!(barrier_id = %key, rule_id = %id_or_rule_id, "Barrier released successfully via rule_id");
            return true;
        }

        let found_key = state
            .active_barriers
            .keys()
            .find(|k| {
                k.strip_prefix("barrier_") == Some(id_or_rule_id)
                    || id_or_rule_id.strip_prefix("barrier_") == Some(k.as_str())
            })
            .cloned();

        if let Some(key) = found_key
            && let Some(barrier) = state.active_barriers.remove(&key)
        {
            let _ = barrier.resume_sender.send(());
            info!(barrier_id = %key, "Barrier released successfully via fallback key match");
            return true;
        }

        false
    }

    pub async fn resume_next_barrier(&self) -> Option<String> {
        let mut state = self.inner.lock().await;
        let oldest_key = state
            .active_barriers
            .iter()
            .min_by_key(|(_, b)| b.reached_at)
            .map(|(k, _)| k.clone());

        if let Some(key) = oldest_key
            && let Some(barrier) = state.active_barriers.remove(&key)
        {
            let _ = barrier.resume_sender.send(());
            info!(barrier_id = %key, "Next oldest barrier released successfully");
            return Some(key);
        }
        None
    }

    pub async fn resume_first(&self) -> Option<String> {
        self.resume_next_barrier().await
    }

    pub async fn record_timeout(&self, barrier_id: &str) {
        let mut state = self.inner.lock().await;
        state.had_timeouts = true;
        if state.active_barriers.remove(barrier_id).is_some() {
            return;
        }

        let key_to_remove = state
            .active_barriers
            .iter()
            .find(|(k, b)| *k == barrier_id || b.rule_id == barrier_id)
            .map(|(k, _)| k.clone());

        if let Some(k) = key_to_remove {
            state.active_barriers.remove(&k);
        }
    }

    pub async fn has_timeouts(&self) -> bool {
        let state = self.inner.lock().await;
        state.had_timeouts
    }

    pub async fn list_active_barriers(&self) -> Vec<ActiveBarrierSummary> {
        let state = self.inner.lock().await;
        state
            .active_barriers
            .values()
            .map(|b| ActiveBarrierSummary {
                id: b.id.clone(),
                rule_id: b.rule_id.clone(),
            })
            .collect()
    }

    #[allow(dead_code)]
    pub async fn active_barrier_ids(&self) -> Vec<String> {
        let state = self.inner.lock().await;
        state.active_barriers.keys().cloned().collect()
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
