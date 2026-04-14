//! An embeddable Rust library for building data pipelines as validated directed
//! acyclic graphs (DAGs), with Apache Arrow `RecordBatch` as the native data
//! format, declarative batching controls, and a single-binary distributed
//! execution model.
//!
//! # Overview
//!
//! Every task receives and produces an Arrow [`RecordBatch`](arrow::record_batch::RecordBatch).
//! Data flows between tasks in a columnar, strongly-typed format — no
//! serialization boundaries, no opaque blobs.
//!
//! Each task declares the columns it **requires**, **produces**, and **drops**.
//! When the pipeline graph is built, `rpl` walks the DAG in topological order
//! and verifies that every task's required columns are available from its
//! upstream producers — catching mismatches before any data is processed.
//!
//! # Batch modes
//!
//! Three built-in [`BatchMode`] variants control how data is grouped before a
//! task runs:
//!
//! - **`Passthrough`** — each incoming batch is forwarded as-is.
//! - **`MaxRows(n)`** — accumulate until the row count reaches *n*, then
//!   concatenate and execute.
//! - **`CommonOrigin`** — wait for contributions from all predecessor branches
//!   sharing the same origin set before executing (barrier for diamond-shaped
//!   graphs).
//!
//! # Executors
//!
//! The [`executor::scheduler::BatchScheduler`] manages queues, origin tracking,
//! accumulation, and flush semantics. It is shared across all executor backends:
//!
//! - [`executor::local::LocalExecutor`] — serial, single-threaded, in-process.
//! - [`executor::threaded::ThreadExecutor`] — multi-threaded within a single process.
//! - [`HqExecutor`] — distributed execution via
//!   [HyperQueue](https://github.com/It4innovations/hyperqueue).
//!
//! All use identical scheduling logic, so local test runs faithfully reproduce
//! distributed behavior.
//!
//! # Data transport
//!
//! The [`DataTransport`] trait abstracts how batches move between tasks:
//!
//! - [`transport::memory::InMemoryTransport`] — for local execution.
//! - [`transport::file::FileTransport`] — Arrow IPC files on a shared filesystem.
//! - [`transport::dispatch::AnyTransport`] — runtime-dispatched variant.
//!
//! # Quick start
//!
//! ```no_run
//! use std::sync::Arc;
//! use arrow::array::Float64Array;
//! use arrow::datatypes::{DataType, Schema};
//! use rpl::{
//!     BatchMode, DefaultGenerator, Executor, PipelineGraph, RecordBatchExt, TaskDef,
//!     executor::local::LocalExecutor,
//!     schema_of,
//! };
//!
//! let mut graph = PipelineGraph::new();
//! graph.add_linear(vec![
//!     TaskDef::new(
//!         "compute_value",
//!         Schema::empty(),
//!         schema_of(&[("value", DataType::Float64)]),
//!         |batch| {
//!             let len = batch.num_rows();
//!             let values: Vec<f64> = (0..len).map(|i| i as f64 * 1.5).collect();
//!             batch.append_column("value", Arc::new(Float64Array::from(values)))
//!         },
//!     ),
//!     TaskDef::new(
//!         "scale",
//!         schema_of(&[("value", DataType::Float64)]),
//!         schema_of(&[("scaled", DataType::Float64)]),
//!         |batch| {
//!             let values = batch.column_as::<Float64Array>("value")?;
//!             let scaled: Float64Array = values.iter()
//!                 .map(|v| v.map(|x| x * 2.0))
//!                 .collect();
//!             batch.append_column("scaled", Arc::new(scaled))
//!         },
//!     ).with_batch_mode(BatchMode::MaxRows(10)),
//! ]).unwrap();
//!
//! let mut executor = LocalExecutor::new().with_max_batches(20);
//! let mut source = DefaultGenerator::new();
//!
//! for result in executor.run(&graph, &mut source).unwrap() {
//!     let output = result.unwrap();
//!     println!("{}: {} rows", output.task, output.data.num_rows());
//! }
//! ```

pub mod batch_ext;
pub mod error;
pub mod executor;
pub mod graph;
pub mod registry;
pub mod schema;
pub mod task;
pub mod tracker;
pub mod transport;

pub use batch_ext::RecordBatchExt;
pub use error::{Result, RplError};
pub use executor::hq::{
    HqBackend, HqClient, HqExecutor, run_worker_if_invoked, run_worker_if_invoked_with,
};
pub use executor::{
    BatchLineage, DefaultGenerator, Executor, OutputBatch, PathStep, SourceGenerator,
};
pub use graph::{Node, PipelineGraph};
pub use registry::TaskRegistry;
pub use schema::schema_of;
pub use task::{BatchMode, Resources, TaskDef};
pub use tracker::{PipelineSummary, PipelineTracker, TaskStats};
pub use transport::DataTransport;
