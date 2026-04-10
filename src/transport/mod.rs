pub mod dispatch;
pub mod file;
pub mod memory;

use std::collections::BTreeSet;

use arrow::record_batch::RecordBatch;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::error::Result;

/// Metadata for a single output produced by a task or split operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutputEntry<H> {
    pub handle: H,
    pub num_rows: usize,
    pub origins: BTreeSet<u64>,
    /// Task execution time in milliseconds (worker-measured).
    /// `None` for split-only operations.
    #[serde(default)]
    pub exec_duration_ms: Option<u64>,
}

/// Abstraction for moving RecordBatch data between tasks.
///
/// Different executors use different transports:
/// - [`memory::InMemoryTransport`] stores batches in a local map (for `LocalExecutor`).
/// - [`file::FileTransport`] writes Arrow IPC files on a shared filesystem (for `HyperQueueExecutor`).
///
/// The `Handle` type is an opaque token that identifies stored data. It must be
/// serializable so it can be passed across process boundaries (e.g. as a CLI
/// argument to a worker process).
///
/// The output protocol (`OutputToken` / `prepare_output` / `publish_output` /
/// `collect_output`) allows workers to report their results back to the driver
/// in a transport-native way — e.g. a manifest file on shared FS, or a
/// FlightInfo query for Arrow Flight.
pub trait DataTransport {
    type Handle: Send + Sync + Clone + Serialize + DeserializeOwned + std::fmt::Debug;

    /// Opaque token the driver creates before submitting a task, passed to
    /// the worker so it can publish its output metadata after execution.
    type OutputToken: Send + Sync + Clone + Serialize + DeserializeOwned + std::fmt::Debug;

    /// Store a RecordBatch, returning a handle to retrieve it later.
    fn store(&self, batch: &RecordBatch) -> Result<Self::Handle>;

    /// Load a RecordBatch by its handle.
    fn load(&self, handle: &Self::Handle) -> Result<RecordBatch>;

    /// Mark a handle as consumed by one consumer.
    ///
    /// For transports with reference counting (e.g. file-based), this decrements
    /// the count and cleans up when it reaches zero. For in-memory transports,
    /// this removes the entry.
    fn release(&self, handle: &Self::Handle) -> Result<()>;

    /// Mark a handle as having multiple consumers (fan-out).
    ///
    /// Call this before the first `release` when a batch is sent to multiple
    /// downstream tasks. The default is 1 consumer.
    fn add_consumers(&self, handle: &Self::Handle, additional: usize) -> Result<()>;

    /// Driver side: create a token the worker will use to publish output metadata.
    fn prepare_output(&self) -> Result<Self::OutputToken>;

    /// Worker side: publish output metadata under the given token.
    fn publish_output(
        &self,
        token: &Self::OutputToken,
        entries: &[OutputEntry<Self::Handle>],
    ) -> Result<()>;

    /// Driver side: retrieve output metadata after a task completes.
    ///
    /// Consumes the token — the underlying resource (e.g. manifest file)
    /// is cleaned up.
    fn collect_output(
        &self,
        token: &Self::OutputToken,
    ) -> Result<Vec<OutputEntry<Self::Handle>>>;
}

/// Blanket implementation so `Arc<T>` can be used as a shared transport.
///
/// This allows `HqExecutor` to hold `Arc<T>` and clone it into the
/// `BatchScheduler` without requiring `T: Clone`.
impl<T: DataTransport> DataTransport for std::sync::Arc<T> {
    type Handle = T::Handle;
    type OutputToken = T::OutputToken;

    fn store(&self, batch: &RecordBatch) -> Result<Self::Handle> {
        (**self).store(batch)
    }

    fn load(&self, handle: &Self::Handle) -> Result<RecordBatch> {
        (**self).load(handle)
    }

    fn release(&self, handle: &Self::Handle) -> Result<()> {
        (**self).release(handle)
    }

    fn add_consumers(&self, handle: &Self::Handle, additional: usize) -> Result<()> {
        (**self).add_consumers(handle, additional)
    }

    fn prepare_output(&self) -> Result<Self::OutputToken> {
        (**self).prepare_output()
    }

    fn publish_output(
        &self,
        token: &Self::OutputToken,
        entries: &[OutputEntry<Self::Handle>],
    ) -> Result<()> {
        (**self).publish_output(token, entries)
    }

    fn collect_output(
        &self,
        token: &Self::OutputToken,
    ) -> Result<Vec<OutputEntry<Self::Handle>>> {
        (**self).collect_output(token)
    }
}
