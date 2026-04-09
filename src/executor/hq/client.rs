use std::collections::HashSet;
use std::path::PathBuf;
use std::process::Command;

use serde::Deserialize;

use crate::error::{Result, RplError};
use crate::task::Resources;

/// Result of a completed HQ task.
#[derive(Debug, Clone)]
pub struct CompletedTask {
    pub task_id: u64,
    pub success: bool,
}

/// Status snapshot from polling an HQ job.
#[derive(Debug)]
pub struct JobPollResult {
    /// Tasks that have finished (successfully or with failure).
    pub completed: Vec<CompletedTask>,
    /// IDs of tasks currently in the running state.
    pub running_ids: HashSet<u64>,
}

/// Backend abstraction for HyperQueue operations.
///
/// Extracted from [`HqClient`] to enable testing with mock backends.
pub trait HqBackend {
    fn create_open_job(&self, name: &str) -> Result<u64>;
    fn submit_task(
        &self,
        job_id: u64,
        command: &[String],
        priority: i32,
        resources: &Resources,
    ) -> Result<u64>;
    fn poll_tasks(&self, job_id: u64) -> Result<JobPollResult>;
    fn close_job(&self, job_id: u64) -> Result<()>;
}

/// Thin wrapper around the HyperQueue CLI (`hq`), using JSON output mode.
pub struct HqClient {
    hq_binary: PathBuf,
}

impl HqClient {
    pub fn new(hq_binary: impl Into<PathBuf>) -> Self {
        HqClient {
            hq_binary: hq_binary.into(),
        }
    }
}

impl HqBackend for HqClient {
    fn create_open_job(&self, name: &str) -> Result<u64> {
        let output = Command::new(&self.hq_binary)
            .args(["job", "submit", "--open", "--name", name, "--output-mode=json"])
            .output()
            .map_err(|e| RplError::Hq(format!("failed to run hq: {e}")))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(RplError::Hq(format!("hq job submit --open failed: {stderr}")));
        }

        let parsed: serde_json::Value = serde_json::from_slice(&output.stdout)
            .map_err(|e| RplError::Hq(format!("failed to parse hq output: {e}")))?;

        parsed["id"]
            .as_u64()
            .ok_or_else(|| RplError::Hq("missing 'id' in hq job submit output".into()))
    }

    fn submit_task(
        &self,
        job_id: u64,
        command: &[String],
        priority: i32,
        resources: &Resources,
    ) -> Result<u64> {
        let mut args = vec![
            "task".to_string(),
            "submit".to_string(),
            format!("--job={job_id}"),
            format!("--priority={priority}"),
            "--output-mode=json".to_string(),
        ];

        // Map resource requirements.
        if resources.num_cpus > 0 {
            args.push(format!("--cpus={}", resources.num_cpus));
        }
        if resources.num_gpus > 0 {
            args.push(format!("--resource=gpus/nvidia={}", resources.num_gpus));
        }
        for (name, count) in &resources.custom {
            args.push(format!("--resource={name}={count}"));
        }

        // Separator and command.
        args.push("--".to_string());
        args.extend_from_slice(command);

        let output = Command::new(&self.hq_binary)
            .args(&args)
            .output()
            .map_err(|e| RplError::Hq(format!("failed to run hq task submit: {e}")))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(RplError::Hq(format!("hq task submit failed: {stderr}")));
        }

        let parsed: serde_json::Value = serde_json::from_slice(&output.stdout)
            .map_err(|e| RplError::Hq(format!("failed to parse hq task submit output: {e}")))?;

        parsed["id"]
            .as_u64()
            .ok_or_else(|| RplError::Hq("missing 'id' in hq task submit output".into()))
    }

    fn poll_tasks(&self, job_id: u64) -> Result<JobPollResult> {
        let output = Command::new(&self.hq_binary)
            .args([
                "task",
                "list",
                &format!("--job={job_id}"),
                "--output-mode=json",
            ])
            .output()
            .map_err(|e| RplError::Hq(format!("failed to run hq task list: {e}")))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(RplError::Hq(format!("hq task list failed: {stderr}")));
        }

        let parsed: Vec<HqTaskInfo> = serde_json::from_slice(&output.stdout)
            .map_err(|e| RplError::Hq(format!("failed to parse hq task list output: {e}")))?;

        let mut completed = Vec::new();
        let mut running_ids = HashSet::new();

        for t in parsed {
            match t.state.as_str() {
                "finished" => completed.push(CompletedTask {
                    task_id: t.id,
                    success: true,
                }),
                "failed" | "canceled" => completed.push(CompletedTask {
                    task_id: t.id,
                    success: false,
                }),
                "running" => {
                    running_ids.insert(t.id);
                }
                _ => {} // waiting, etc.
            }
        }

        Ok(JobPollResult {
            completed,
            running_ids,
        })
    }

    fn close_job(&self, job_id: u64) -> Result<()> {
        let output = Command::new(&self.hq_binary)
            .args(["job", "close", &format!("{job_id}")])
            .output()
            .map_err(|e| RplError::Hq(format!("failed to run hq job close: {e}")))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(RplError::Hq(format!("hq job close failed: {stderr}")));
        }

        Ok(())
    }
}

#[derive(Deserialize)]
struct HqTaskInfo {
    id: u64,
    state: String,
}
