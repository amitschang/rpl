use std::collections::HashMap;
use std::sync::Mutex;

use arrow::record_batch::RecordBatch;
use serde::{Deserialize, Serialize};

use crate::error::{Result, RplError};
use crate::transport::{DataTransport, OutputEntry, RefCountMap};

/// A handle into the in-memory store, identified by a monotonic u64 key.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct MemoryHandle(u64);

/// Output token for in-memory transport.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryOutputToken(u64);

struct Entry {
    batch: RecordBatch,
}

/// In-memory transport that stores RecordBatches in a HashMap.
///
/// Suitable for single-process executors like [`LocalExecutor`].
/// Not suitable for distributed execution since handles are
/// process-local.
pub struct InMemoryTransport {
    store: Mutex<HashMap<u64, Entry>>,
    ref_counts: Mutex<RefCountMap<u64>>,
    output_store: Mutex<HashMap<u64, Vec<OutputEntry<MemoryHandle>>>>,
    next_id: Mutex<u64>,
}

impl InMemoryTransport {
    pub fn new() -> Self {
        InMemoryTransport {
            store: Mutex::new(HashMap::new()),
            ref_counts: Mutex::new(RefCountMap::new()),
            output_store: Mutex::new(HashMap::new()),
            next_id: Mutex::new(0),
        }
    }

    fn next_id(&self) -> u64 {
        let mut next = self.next_id.lock().unwrap();
        let id = *next;
        *next += 1;
        id
    }
}

impl Default for InMemoryTransport {
    fn default() -> Self {
        Self::new()
    }
}

impl DataTransport for InMemoryTransport {
    type Handle = MemoryHandle;
    type OutputToken = MemoryOutputToken;

    fn store(&self, batch: &RecordBatch) -> Result<Self::Handle> {
        let id = self.next_id();

        let mut store = self.store.lock().unwrap();
        store.insert(id, Entry { batch: batch.clone() });
        self.ref_counts.lock().unwrap().insert(id);
        Ok(MemoryHandle(id))
    }

    fn load(&self, handle: &Self::Handle) -> Result<RecordBatch> {
        let store = self.store.lock().unwrap();
        store
            .get(&handle.0)
            .map(|e| e.batch.clone())
            .ok_or_else(|| RplError::Transport(format!("handle {:?} not found", handle)))
    }

    fn release(&self, handle: &Self::Handle) -> Result<()> {
        let mut refs = self.ref_counts.lock().unwrap();
        if refs.release(&handle.0) {
            self.store.lock().unwrap().remove(&handle.0);
        }
        Ok(())
    }

    fn add_consumers(&self, handle: &Self::Handle, additional: usize) -> Result<()> {
        self.ref_counts.lock().unwrap().add_consumers(&handle.0, additional);
        Ok(())
    }

    fn prepare_output(&self) -> Result<Self::OutputToken> {
        Ok(MemoryOutputToken(self.next_id()))
    }

    fn publish_output(
        &self,
        token: &Self::OutputToken,
        entries: &[OutputEntry<Self::Handle>],
    ) -> Result<()> {
        let mut store = self.output_store.lock().unwrap();
        store.insert(token.0, entries.to_vec());
        Ok(())
    }

    fn collect_output(
        &self,
        token: &Self::OutputToken,
    ) -> Result<Vec<OutputEntry<Self::Handle>>> {
        let mut store = self.output_store.lock().unwrap();
        store.remove(&token.0).ok_or_else(|| {
            RplError::Transport(format!("output token {:?} not found", token))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::test_batch;

    #[test]
    fn store_load_release() {
        let transport = InMemoryTransport::new();
        let batch = test_batch();

        let handle = transport.store(&batch).unwrap();
        let loaded = transport.load(&handle).unwrap();
        assert_eq!(loaded.num_rows(), 3);

        transport.release(&handle).unwrap();
        assert!(transport.load(&handle).is_err());
    }

    #[test]
    fn multi_consumer() {
        let transport = InMemoryTransport::new();
        let batch = test_batch();

        let handle = transport.store(&batch).unwrap();
        transport.add_consumers(&handle, 1).unwrap(); // now 2 consumers

        transport.release(&handle).unwrap(); // ref_count -> 1
        assert!(transport.load(&handle).is_ok()); // still available

        transport.release(&handle).unwrap(); // ref_count -> 0
        assert!(transport.load(&handle).is_err()); // cleaned up
    }
}
