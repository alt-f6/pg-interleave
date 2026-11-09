use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::{BufReader, BufWriter};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::{Mutex, Notify};
use tracing::info;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TraceStep {
    pub step: usize,
    pub conn_id: u64,
    pub query: String,
    pub params: Vec<Option<String>>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct TraceLog {
    pub steps: Vec<TraceStep>,
}

#[derive(Clone)]
pub struct TraceRecorder {
    path: PathBuf,
    steps: Arc<Mutex<Vec<TraceStep>>>,
}

impl TraceRecorder {
    pub fn new(path: impl AsRef<Path>) -> Self {
        Self {
            path: path.as_ref().to_path_buf(),
            steps: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub async fn record(&self, conn_id: u64, query: &str, params: &[Option<String>]) {
        let mut steps = self.steps.lock().await;
        let step = steps.len();
        steps.push(TraceStep {
            step,
            conn_id,
            query: query.to_string(),
            params: params.to_vec(),
        });
    }

    pub async fn flush(&self) -> anyhow::Result<()> {
        let steps = self.steps.lock().await;
        let log = TraceLog {
            steps: steps.clone(),
        };
        let file = File::create(&self.path)?;
        let writer = BufWriter::new(file);
        serde_json::to_writer_pretty(writer, &log)?;
        info!(path = ?self.path, count = log.steps.len(), "Trace recording flushed to file");
        Ok(())
    }

    pub async fn clear(&self) {
        let mut steps = self.steps.lock().await;
        steps.clear();
    }
}

pub struct TraceReplayer {
    steps: Vec<TraceStep>,
    current_step: AtomicUsize,
    notify: Arc<Notify>,
}

impl TraceReplayer {
    pub fn from_file(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let file = File::open(path)?;
        let reader = BufReader::new(file);
        let log: TraceLog = serde_json::from_reader(reader)?;
        info!(count = log.steps.len(), "Loaded execution trace for replay");
        Ok(Self {
            steps: log.steps,
            current_step: AtomicUsize::new(0),
            notify: Arc::new(Notify::new()),
        })
    }

    pub async fn wait_for_turn(&self, conn_id: u64, query: &str) {
        loop {
            let step_idx = self.current_step.load(Ordering::SeqCst);
            if step_idx >= self.steps.len() {
                return;
            }

            let expected = &self.steps[step_idx];

            if expected.conn_id == conn_id && expected.query == query {
                return;
            }

            self.notify.notified().await;
        }
    }

    pub fn advance_step(&self) {
        self.current_step.fetch_add(1, Ordering::SeqCst);
        self.notify.notify_waiters();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn test_trace_recorder_and_replayer_step_ordering() {
        let temp_dir = tempfile::tempdir().unwrap();
        let trace_path = temp_dir.path().join("trace.json");

        let recorder = TraceRecorder::new(&trace_path);
        recorder.record(1, "BEGIN", &[]).await;
        recorder.record(2, "SELECT 1", &[]).await;
        recorder
            .record(1, "UPDATE accounts", &[Some("42".to_string())])
            .await;
        recorder.record(1, "COMMIT", &[]).await;
        recorder.flush().await.unwrap();

        let replayer = Arc::new(TraceReplayer::from_file(&trace_path).unwrap());

        let replayer_clone = replayer.clone();
        let conn2_task = tokio::spawn(async move {
            replayer_clone.wait_for_turn(2, "SELECT 1").await;
            true
        });

        tokio::time::sleep(Duration::from_millis(30)).await;
        assert!(!conn2_task.is_finished());

        replayer.advance_step();

        let conn2_res = tokio::time::timeout(Duration::from_millis(100), conn2_task).await;
        assert!(conn2_res.unwrap().unwrap());

        replayer.advance_step();
        replayer.advance_step();
        replayer.advance_step();

        replayer.wait_for_turn(99, "SELECT 99").await;
    }
}
