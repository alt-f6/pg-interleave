use regex::Regex;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{Mutex, oneshot};
use tracing::info;

#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum BarrierAction {
    PauseBeforeQuery,
    PauseAfterResponse,
}

#[derive(Debug, Clone, Deserialize)]
pub struct InterceptRule {
    pub target_pid: Option<u32>,
    pub query_pattern: String,
    #[allow(dead_code)]
    pub action: BarrierAction,
}

#[allow(dead_code)]
pub struct ActiveBarrier {
    pub id: String,
    pub rule: InterceptRule,
    pub reached: bool,
    pub resume_sender: Option<oneshot::Sender<()>>,
}

#[derive(Default)]
pub struct BarrierState {
    pub rules: Vec<InterceptRule>,
    pub active_barriers: HashMap<String, ActiveBarrier>,
    pub pid_to_conn: HashMap<u32, u64>,
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

    pub async fn check_and_create_barrier(
        &self,
        pid: Option<u32>,
        query: &str,
    ) -> Option<(String, oneshot::Receiver<()>)> {
        let mut state = self.0.lock().await;

        let mut matched_rule: Option<InterceptRule> = None;
        for rule in &state.rules {
            if let Some(target_pid) = rule.target_pid
                && Some(target_pid) != pid {
                    continue;
                }

            if let Ok(re) = Regex::new(&rule.query_pattern)
                && re.is_match(query) {
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
                    reached: true,
                    resume_sender: Some(tx),
                },
            );

            info!(barrier_id = %barrier_id, query = %query, "Barrier reached!");
            return Some((barrier_id, rx));
        }

        None
    }

    /// Resumes execution for a specific barrier by its ID
    pub async fn resume_barrier(&self, barrier_id: &str) -> bool {
        let mut state = self.0.lock().await;
        if let Some(mut barrier) = state.active_barriers.remove(barrier_id)
            && let Some(tx) = barrier.resume_sender.take() {
                let _ = tx.send(());
                info!(barrier_id = %barrier_id, "Barrier released successfully");
                return true;
            }
        false
    }

    /// Resumes execution of the first active barrier in the queue
    pub async fn resume_first(&self) -> Option<String> {
        let mut state = self.0.lock().await;
        let first_key = state.active_barriers.keys().next().cloned()?;
        if let Some(mut barrier) = state.active_barriers.remove(&first_key)
            && let Some(tx) = barrier.resume_sender.take() {
                let _ = tx.send(());
                info!(barrier_id = %first_key, "First active barrier in queue released successfully");
                return Some(first_key);
            }
        None
    }
}
