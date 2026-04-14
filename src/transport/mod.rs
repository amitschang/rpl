//! Pluggable data transport for moving batches between tasks.
//!
//! The [`DataTransport`] trait abstracts batch storage with explicit
//! lifecycle management (store, load, release, fan-out reference
//! counting). Built-in implementations:
//!
//! - [`memory::InMemoryTransport`] — in-process storage for local execution.
//! - [`file::FileTransport`] — Arrow IPC files on a shared filesystem.
//! - [`dispatch::AnyTransport`] — runtime-dispatched variant for
//!   configuration-driven selection.

pub mod dispatch;
pub mod file;
pub mod memory;

use std::collections::{BTreeSet, HashMap};
use std::hash::Hash;

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

impl<H> OutputEntry<H> {
    /// Transform the handle type, preserving all other fields.
    pub fn map_handle<U>(self, f: impl FnOnce(H) -> U) -> OutputEntry<U> {
        OutputEntry {
            handle: f(self.handle),
            num_rows: self.num_rows,
            origins: self.origins,
            exec_duration_ms: self.exec_duration_ms,
        }
    }
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
    fn collect_output(&self, token: &Self::OutputToken) -> Result<Vec<OutputEntry<Self::Handle>>>;
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

    fn collect_output(&self, token: &Self::OutputToken) -> Result<Vec<OutputEntry<Self::Handle>>> {
        (**self).collect_output(token)
    }
}

/// Reference-counted handle map used by transport implementations.
///
/// Tracks how many consumers hold a reference to each key. When the count
/// reaches zero the entry is removed and an optional cleanup callback fires.
pub(crate) struct RefCountMap<K> {
    counts: HashMap<K, usize>,
}

impl<K: Eq + Hash + Clone> RefCountMap<K> {
    pub fn new() -> Self {
        Self {
            counts: HashMap::new(),
        }
    }

    /// Insert a key with a reference count of 1.
    pub fn insert(&mut self, key: K) {
        self.counts.insert(key, 1);
    }

    /// Increment the reference count by `additional`.
    pub fn add_consumers(&mut self, key: &K, additional: usize) {
        if let Some(count) = self.counts.get_mut(key) {
            *count += additional;
        }
    }

    /// Decrement the reference count. Returns `true` if the count reached
    /// zero and the entry was removed (caller should clean up resources).
    pub fn release(&mut self, key: &K) -> bool {
        if let Some(count) = self.counts.get_mut(key) {
            *count -= 1;
            if *count == 0 {
                self.counts.remove(key);
                return true;
            }
        }
        false
    }
}

#[cfg(test)]
pub(crate) fn test_batch() -> arrow::record_batch::RecordBatch {
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
    let col = Arc::new(Int32Array::from(vec![1, 2, 3]));
    arrow::record_batch::RecordBatch::try_new(schema, vec![col]).unwrap()
}
