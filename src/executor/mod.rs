pub mod hq;
pub mod local;
pub mod scheduler;

use std::collections::BTreeSet;
use std::sync::{Arc, atomic};
use std::time::Duration;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use crate::error::Result;
use crate::graph::PipelineGraph;

/// A source of seed RecordBatches fed into source tasks.
///
/// Implementations declare what columns they produce via [`produces()`](SourceGenerator::produces),
/// which is used by [`PipelineGraph::validate`] to seed the schema walk.
pub trait SourceGenerator: Send {
    /// Schema of the batches this source produces.
    fn produces(&self) -> &Schema;

    /// Generate the next batch, or `None` if exhausted.
    fn next_batch(&mut self) -> Option<RecordBatch>;
}

/// Default source generator producing a single `id` (Int64) column,
/// incrementing from 0 on each call. Never exhausts.
pub struct DefaultGenerator {
    schema: Schema,
    id: i64,
}

impl DefaultGenerator {
    pub fn new() -> Self {
        DefaultGenerator {
            schema: Schema::new(vec![Field::new("id", DataType::Int64, false)]),
            id: 0,
        }
    }
}

impl Default for DefaultGenerator {
    fn default() -> Self {
        Self::new()
    }
}

impl SourceGenerator for DefaultGenerator {
    fn produces(&self) -> &Schema {
        &self.schema
    }

    fn next_batch(&mut self) -> Option<RecordBatch> {
        let batch = RecordBatch::try_new(
            Arc::new(self.schema.clone()),
            vec![Arc::new(Int64Array::from(vec![self.id]))],
        )
        .unwrap();
        self.id += 1;
        Some(batch)
    }
}

/// Global counter for unique execution IDs.
static NEXT_EXEC_ID: atomic::AtomicU64 =  atomic::AtomicU64::new(0);

/// Allocate a globally unique execution ID for a [`PathStep`].
pub fn next_exec_id() -> u64 {
    NEXT_EXEC_ID.fetch_add(1, atomic::Ordering::Relaxed)
}

/// One step in a batch's journey through the DAG.
#[derive(Debug, Clone)]
pub struct PathStep {
    /// Unique identifier for this particular execution.
    /// Two output batches that share the same `exec_id` for a step
    /// went through that step in the same physical execution (e.g. fan-out).
    pub exec_id: u64,
    /// Name of the task that processed this batch.
    pub task: String,
    /// Pure execution time as measured by the worker.
    pub exec_duration: Duration,
    /// Wall time from submit to completion as seen by the driver.
    pub wall_duration: Duration,
}

/// Per-batch lineage accumulated as it flows through the pipeline.
#[derive(Debug, Clone, Default)]
pub struct BatchLineage {
    /// Source batch IDs that contributed to this batch.
    pub origins: BTreeSet<u64>,
    /// Ordered sequence of tasks this batch passed through.
    pub path: Vec<PathStep>,
}

/// A completed output batch from a sink task.
#[derive(Debug)]
pub struct OutputBatch {
    pub data: RecordBatch,
    pub task: String,
    pub lineage: BatchLineage,
}

/// Trait for pipeline execution backends.
///
/// An executor takes a validated [`PipelineGraph`] and runs it, yielding
/// [`OutputBatch`]es from the sink tasks as they become available.
///
/// Implementations differ in how they schedule and run tasks:
/// - [`local::LocalExecutor`]: serial, single-threaded, in-memory.
/// - (future) `HyperQueueExecutor`: distributed via HyperQueue CLI.
pub trait Executor {
    /// Execute the pipeline, returning an iterator of output batches.
    ///
    /// The iterator may be infinite (streaming mode) or finite depending
    /// on the source generator and `max_batches` configuration.
    fn run<'a>(
        &'a mut self,
        graph: &'a PipelineGraph,
        source: &'a mut dyn SourceGenerator,
    ) -> Result<Box<dyn Iterator<Item = Result<OutputBatch>> + 'a>>;
}
