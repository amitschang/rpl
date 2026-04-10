//! Pipeline execution tracker for observability.
//!
//! [`PipelineTracker`] aggregates timing and throughput statistics from
//! [`OutputBatch`] lineage as batches flow through the pipeline.  It
//! deduplicates by [`PathStep::exec_id`] so fan-out and merge points are
//! counted exactly once.
//!
//! # Usage
//!
//! ```ignore
//! use rpl::{PipelineTracker, OutputBatch};
//!
//! let mut tracker = PipelineTracker::new(task_names);
//!
//! for result in executor.run(&graph, &mut source)? {
//!     let output = result?;
//!     tracker.update_and_report(&output);   // live ANSI progress on stderr
//!     // … or tracker.update(&output);      // silent accumulation
//! }
//! tracker.finish();        // clear live output
//! tracker.print_summary(); // final report on stdout
//! ```

use std::collections::{HashMap, HashSet};
use std::io::{self, Write};
use std::time::{Duration, Instant};

use crate::executor::OutputBatch;

/// Per-task accumulated statistics.
#[derive(Debug, Clone, Default)]
pub struct TaskStats {
    /// Number of unique executions of this task.
    pub runs: usize,
    /// Sum of pure execution time across all runs.
    pub exec_duration: Duration,
    /// Sum of wall time (submit → complete) across all runs.
    pub wall_duration: Duration,
}

/// Aggregated pipeline execution statistics.
///
/// Returned by [`PipelineTracker::summary`].
#[derive(Debug, Clone)]
pub struct PipelineSummary {
    /// Per-task statistics, keyed by task name.
    pub per_task: HashMap<String, TaskStats>,
    /// Task names in the order they were registered.
    pub task_order: Vec<String>,
    /// Total number of sink batches observed.
    pub sink_batches: usize,
    /// Wall time elapsed since the tracker was created.
    pub elapsed: Duration,
    /// Sum of all per-task execution time.
    pub total_exec: Duration,
    /// Sum of all per-task wall time.
    pub total_wall: Duration,
    /// Effective parallelism: `total_exec / elapsed`.
    ///
    /// A value of 1.0 means purely serial execution; higher values indicate
    /// tasks ran in parallel.  Only meaningful when `elapsed > 0`.
    pub parallelism: f64,
}

/// Pipeline execution tracker.
///
/// Accumulates per-task timing from [`OutputBatch`] lineage, deduplicating
/// by `exec_id` so that fan-out copies are counted once.
pub struct PipelineTracker {
    per_task: HashMap<String, TaskStats>,
    seen: HashSet<u64>,
    task_order: Vec<String>,
    lines_printed: usize,
    start: Instant,
    sink_count: usize,
}

impl PipelineTracker {
    /// Create a new tracker.
    ///
    /// `task_names` controls the display order in reports; any task not
    /// listed here will still be tracked but appended at the end.
    pub fn new(task_names: Vec<String>) -> Self {
        let mut per_task = HashMap::new();
        for name in &task_names {
            per_task.insert(name.clone(), TaskStats::default());
        }
        PipelineTracker {
            per_task,
            seen: HashSet::new(),
            task_order: task_names,
            lines_printed: 0,
            start: Instant::now(),
            sink_count: 0,
        }
    }

    /// Silently record statistics from an output batch.
    pub fn update(&mut self, output: &OutputBatch) {
        self.sink_count += 1;
        self.ingest_lineage(output);
    }

    /// Record statistics and refresh a live progress display on stderr.
    pub fn update_and_report(&mut self, output: &OutputBatch) {
        self.sink_count += 1;
        self.ingest_lineage(output);
        self.render();
    }

    /// Return a snapshot of the current statistics.
    pub fn summary(&self) -> PipelineSummary {
        let elapsed = self.start.elapsed();
        let total_exec: Duration = self.per_task.values().map(|s| s.exec_duration).sum();
        let total_wall: Duration = self.per_task.values().map(|s| s.wall_duration).sum();
        let parallelism = if elapsed.as_nanos() > 0 {
            total_exec.as_secs_f64() / elapsed.as_secs_f64()
        } else {
            0.0
        };

        PipelineSummary {
            per_task: self.per_task.clone(),
            task_order: self.task_order.clone(),
            sink_batches: self.sink_count,
            elapsed,
            total_exec,
            total_wall,
            parallelism,
        }
    }

    /// Print a formatted summary table to stdout.
    pub fn print_summary(&self) {
        let mut out = io::stdout().lock();
        self.write_table(&mut out, "");
        out.flush().ok();
    }

    /// Finalize live output: print a newline so subsequent output doesn't
    /// overwrite the last progress line.
    pub fn finish(&self) {
        eprintln!();
    }

    // -- internals ----------------------------------------------------------

    fn ingest_lineage(&mut self, output: &OutputBatch) {
        for step in &output.lineage.path {
            if self.seen.insert(step.exec_id) {
                let entry = self
                    .per_task
                    .entry(step.task.clone())
                    .or_default();
                entry.runs += 1;
                entry.exec_duration += step.exec_duration;
                entry.wall_duration += step.wall_duration;
                // Ensure dynamically discovered tasks appear in order.
                if !self.task_order.contains(&step.task) {
                    self.task_order.push(step.task.clone());
                }
            }
        }
    }

    /// Write the stats table to `out`.  When `line_prefix` is non-empty each
    /// line is prefixed with it (used by `render` to inject ANSI clear codes).
    fn write_table(&self, out: &mut impl Write, line_prefix: &str) -> usize {
        let elapsed = self.start.elapsed();
        let total_exec: Duration = self.per_task.values().map(|s| s.exec_duration).sum();
        let total_wall: Duration = self.per_task.values().map(|s| s.wall_duration).sum();
        let parallelism = if elapsed.as_nanos() > 0 {
            total_exec.as_secs_f64() / elapsed.as_secs_f64()
        } else {
            0.0
        };

        let mut lines = 0usize;

        writeln!(
            out,
            "{line_prefix}  elapsed: {elapsed:.1?}  sink batches: {}  \
             total exec: {total_exec:.1?}  parallelism: {parallelism:.1}x",
            self.sink_count,
        )
        .ok();
        lines += 1;

        writeln!(
            out,
            "{line_prefix}  {:<15} {:>6} {:>12} {:>8} {:>12} {:>8}",
            "TASK", "RUNS", "EXEC", "% EXEC", "WALL", "% WALL"
        )
        .ok();
        lines += 1;

        writeln!(out, "{line_prefix}  {}", "-".repeat(66)).ok();
        lines += 1;

        for name in &self.task_order {
            let stats = self.per_task.get(name).cloned().unwrap_or_default();
            let exec_pct = pct(stats.exec_duration, total_exec);
            let wall_pct = pct(stats.wall_duration, total_wall);
            writeln!(
                out,
                "{line_prefix}  {name:<15} {:>6} {:>12.1?} {exec_pct:>7.1}% {:>12.1?} {wall_pct:>7.1}%",
                stats.runs, stats.exec_duration, stats.wall_duration,
            )
            .ok();
            lines += 1;
        }

        writeln!(out, "{line_prefix}  {}", "-".repeat(66)).ok();
        lines += 1;

        writeln!(
            out,
            "{line_prefix}  {:<15} {:>6} {:>12.1?} {:>8} {:>12.1?}",
            "TOTAL", "", total_exec, "100.0%", total_wall,
        )
        .ok();
        lines += 1;

        lines
    }

    fn render(&mut self) {
        let mut out = io::stderr().lock();
        if self.lines_printed > 0 {
            write!(out, "\x1b[{}A", self.lines_printed).ok();
        }
        self.lines_printed = self.write_table(&mut out, "\x1b[2K");
        out.flush().ok();
    }
}

fn pct(part: Duration, total: Duration) -> f64 {
    if total.as_nanos() > 0 {
        part.as_secs_f64() / total.as_secs_f64() * 100.0
    } else {
        0.0
    }
}
