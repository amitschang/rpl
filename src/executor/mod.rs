pub mod local;
pub mod scheduler;

use std::sync::Arc;

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

/// A completed output batch from a sink task.
#[derive(Debug)]
pub struct OutputBatch {
    pub data: RecordBatch,
    pub task: String,
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
