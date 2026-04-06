pub mod file;
pub mod memory;

use arrow::record_batch::RecordBatch;
use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::error::Result;

/// Abstraction for moving RecordBatch data between tasks.
///
/// Different executors use different transports:
/// - [`memory::InMemoryTransport`] stores batches in a local map (for `LocalExecutor`).
/// - [`file::FileTransport`] writes Arrow IPC files on a shared filesystem (for `HyperQueueExecutor`).
///
/// The `Handle` type is an opaque token that identifies stored data. It must be
/// serializable so it can be passed across process boundaries (e.g. as a CLI
/// argument to a worker process).
pub trait DataTransport {
    type Handle: Send + Sync + Clone + Serialize + DeserializeOwned + std::fmt::Debug;

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
}
