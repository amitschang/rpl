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
                // Convert AnyHandle entries to FileHandle entries.
                let file_entries: Vec<OutputEntry<FileHandle>> = entries
                    .iter()
                    .map(|e| match &e.handle {
                        AnyHandle::File(h) => OutputEntry {
                            handle: h.clone(),
                            num_rows: e.num_rows,
                            origins: e.origins.clone(),
                        },
                    })
                    .collect();
                t.publish_output(tok, &file_entries)
            }
        }
    }

    fn collect_output(
        &self,
        token: &Self::OutputToken,
    ) -> Result<Vec<OutputEntry<Self::Handle>>> {
        match (self, token) {
            (AnyTransport::File(t), AnyOutputToken::File(tok)) => {
                let file_entries = t.collect_output(tok)?;
                Ok(file_entries
                    .into_iter()
                    .map(|e| OutputEntry {
                        handle: AnyHandle::File(e.handle),
                        num_rows: e.num_rows,
                        origins: e.origins,
                    })
                    .collect())
            }
        }
    }
}
