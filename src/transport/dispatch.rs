//! Runtime-dispatch transport: choose the transport variant at runtime.
//!
//! `AnyTransport` wraps a concrete transport behind an enum, erasing the
//! type at compile time while still implementing [`DataTransport`].
//!
//! This is useful when the transport kind is determined by user
//! configuration (e.g. a CLI flag like `--transport file`) rather than
//! being known statically.

use arrow::record_batch::RecordBatch;
use serde::{Deserialize, Serialize};

use crate::error::Result;
use crate::transport::file::{FileHandle, FileOutputToken, FileTransport};
use crate::transport::{DataTransport, OutputEntry};

/// A handle that can represent any transport's handle at runtime.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "transport", content = "handle")]
pub enum AnyHandle {
    File(FileHandle),
}

/// An output token that can represent any transport's token at runtime.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "transport", content = "token")]
pub enum AnyOutputToken {
    File(FileOutputToken),
}

/// A transport that dispatches to a concrete variant at runtime.
pub enum AnyTransport {
    File(FileTransport),
}

impl AnyTransport {
    /// Create a file-based transport from a staging directory.
    pub fn file(staging_dir: impl Into<std::path::PathBuf>) -> Result<Self> {
        Ok(AnyTransport::File(FileTransport::new(staging_dir)?))
    }
}

impl DataTransport for AnyTransport {
    type Handle = AnyHandle;
    type OutputToken = AnyOutputToken;

    fn store(&self, batch: &RecordBatch) -> Result<Self::Handle> {
        match self {
            AnyTransport::File(t) => t.store(batch).map(AnyHandle::File),
        }
    }

    fn load(&self, handle: &Self::Handle) -> Result<RecordBatch> {
        match (self, handle) {
            (AnyTransport::File(t), AnyHandle::File(h)) => t.load(h),
        }
    }

    fn release(&self, handle: &Self::Handle) -> Result<()> {
        match (self, handle) {
            (AnyTransport::File(t), AnyHandle::File(h)) => t.release(h),
        }
    }

    fn add_consumers(&self, handle: &Self::Handle, additional: usize) -> Result<()> {
        match (self, handle) {
            (AnyTransport::File(t), AnyHandle::File(h)) => t.add_consumers(h, additional),
        }
    }

    fn prepare_output(&self) -> Result<Self::OutputToken> {
        match self {
            AnyTransport::File(t) => t.prepare_output().map(AnyOutputToken::File),
        }
    }

    fn publish_output(
        &self,
        token: &Self::OutputToken,
        entries: &[OutputEntry<Self::Handle>],
    ) -> Result<()> {
        match (self, token) {
            (AnyTransport::File(t), AnyOutputToken::File(tok)) => {
                let file_entries: Vec<OutputEntry<FileHandle>> = entries
                    .iter()
                    .cloned()
                    .map(|e| {
                        e.map_handle(|h| match h {
                            AnyHandle::File(fh) => fh,
                        })
                    })
                    .collect();
                t.publish_output(tok, &file_entries)
            }
        }
    }

    fn collect_output(&self, token: &Self::OutputToken) -> Result<Vec<OutputEntry<Self::Handle>>> {
        match (self, token) {
            (AnyTransport::File(t), AnyOutputToken::File(tok)) => {
                let file_entries = t.collect_output(tok)?;
                Ok(file_entries
                    .into_iter()
                    .map(|e| e.map_handle(AnyHandle::File))
                    .collect())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::test_batch;
    use arrow::array::Int32Array;
    use std::collections::BTreeSet;

    #[test]
    fn store_load_release_roundtrip() {
        let tmp = tempfile::tempdir().unwrap();
        let transport = AnyTransport::file(tmp.path()).unwrap();
        let batch = test_batch();

        let handle = transport.store(&batch).unwrap();
        let loaded = transport.load(&handle).unwrap();
        assert_eq!(loaded.num_rows(), 3);
        assert_eq!(
            loaded
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values(),
            &[1, 2, 3],
        );

        transport.release(&handle).unwrap();
        // After release, loading should fail.
        assert!(transport.load(&handle).is_err());
    }

    #[test]
    fn publish_collect_output_roundtrip() {
        let tmp = tempfile::tempdir().unwrap();
        let transport = AnyTransport::file(tmp.path()).unwrap();
        let batch = test_batch();

        let handle = transport.store(&batch).unwrap();
        let token = transport.prepare_output().unwrap();

        let entries = vec![OutputEntry {
            handle: handle.clone(),
            num_rows: 3,
            origins: BTreeSet::from([0, 1]),
            exec_duration_ms: Some(42),
        }];

        transport.publish_output(&token, &entries).unwrap();
        let collected = transport.collect_output(&token).unwrap();

        assert_eq!(collected.len(), 1);
        assert_eq!(collected[0].num_rows, 3);
        assert_eq!(collected[0].origins, BTreeSet::from([0, 1]));
        assert_eq!(collected[0].exec_duration_ms, Some(42));

        // Verify we can load the data via the collected handle.
        let loaded = transport.load(&collected[0].handle).unwrap();
        assert_eq!(loaded.num_rows(), 3);

        transport.release(&handle).unwrap();
    }

    #[test]
    fn fan_out_with_consumers() {
        let tmp = tempfile::tempdir().unwrap();
        let transport = AnyTransport::file(tmp.path()).unwrap();
        let batch = test_batch();

        let handle = transport.store(&batch).unwrap();
        transport.add_consumers(&handle, 2).unwrap(); // total 3 consumers

        transport.release(&handle).unwrap();
        assert!(transport.load(&handle).is_ok()); // still 2 consumers
        transport.release(&handle).unwrap();
        assert!(transport.load(&handle).is_ok()); // still 1 consumer
        transport.release(&handle).unwrap();
        assert!(transport.load(&handle).is_err()); // cleaned up
    }
}
