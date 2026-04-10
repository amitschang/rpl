use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use arrow::ipc::reader::FileReader;
use arrow::ipc::writer::FileWriter;
use arrow::record_batch::RecordBatch;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::error::{Result, RplError};
use crate::transport::{DataTransport, OutputEntry};

/// Handle for file-based transport: just a path to an Arrow IPC file.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct FileHandle(PathBuf);

impl FileHandle {
    pub fn path(&self) -> &Path {
        &self.0
    }
}

/// Output token for file-based transport: path to a manifest JSON file.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileOutputToken(PathBuf);

impl FileOutputToken {
    pub fn path(&self) -> &Path {
        &self.0
    }
}

/// File-based transport that writes RecordBatches as Arrow IPC files.
///
/// Suitable for distributed executors where tasks run in separate processes
/// with access to a shared filesystem.
pub struct FileTransport {
    staging_dir: PathBuf,
    ref_counts: Mutex<HashMap<PathBuf, usize>>,
}

impl FileTransport {
    pub fn new(staging_dir: impl Into<PathBuf>) -> Result<Self> {
        let dir = staging_dir.into();
        fs::create_dir_all(&dir)?;
        Ok(FileTransport {
            staging_dir: dir,
            ref_counts: Mutex::new(HashMap::new()),
        })
    }

    /// Create a FileTransport in a temporary directory.
    pub fn temp() -> Result<Self> {
        let dir = std::env::temp_dir().join(format!("rpl-staging-{}", Uuid::new_v4()));
        Self::new(dir)
    }
}

impl DataTransport for FileTransport {
    type Handle = FileHandle;
    type OutputToken = FileOutputToken;

    fn store(&self, batch: &RecordBatch) -> Result<Self::Handle> {
        let filename = format!("{}.arrow", Uuid::new_v4());
        let path = self.staging_dir.join(filename);

        let file = fs::File::create(&path)?;
        let mut writer = FileWriter::try_new(file, &batch.schema())?;
        writer.write(batch)?;
        writer.finish()?;

        let mut refs = self.ref_counts.lock().unwrap();
        refs.insert(path.clone(), 1);

        Ok(FileHandle(path))
    }

    fn load(&self, handle: &Self::Handle) -> Result<RecordBatch> {
        let file = fs::File::open(&handle.0).map_err(|e| {
            RplError::Transport(format!("failed to open {}: {}", handle.0.display(), e))
        })?;
        let reader = FileReader::try_new(file, None)?;
        let schema = reader.schema();
        let batches: std::result::Result<Vec<_>, _> = reader.collect();
        arrow::compute::concat_batches(&schema, &batches?).map_err(RplError::Arrow)
    }

    fn release(&self, handle: &Self::Handle) -> Result<()> {
        let mut refs = self.ref_counts.lock().unwrap();
        if let Some(count) = refs.get_mut(&handle.0) {
            *count -= 1;
            if *count == 0 {
                refs.remove(&handle.0);
                if handle.0.exists() {
                    fs::remove_file(&handle.0)?;
                }
            }
        }
        Ok(())
    }

    fn add_consumers(&self, handle: &Self::Handle, additional: usize) -> Result<()> {
        let mut refs = self.ref_counts.lock().unwrap();
        if let Some(count) = refs.get_mut(&handle.0) {
            *count += additional;
        }
        Ok(())
    }

    fn prepare_output(&self) -> Result<Self::OutputToken> {
        let path = self
            .staging_dir
            .join(format!("manifest-{}.json", Uuid::new_v4()));
        Ok(FileOutputToken(path))
    }

    fn publish_output(
        &self,
        token: &Self::OutputToken,
        entries: &[OutputEntry<Self::Handle>],
    ) -> Result<()> {
        let json = serde_json::to_string(entries)
            .map_err(|e| RplError::Hq(format!("failed to serialize output entries: {e}")))?;
        fs::write(&token.0, json)?;
        Ok(())
    }

    fn collect_output(
        &self,
        token: &Self::OutputToken,
    ) -> Result<Vec<OutputEntry<Self::Handle>>> {
        let json = fs::read_to_string(&token.0).map_err(|e| {
            RplError::Hq(format!(
                "failed to read manifest {}: {e}",
                token.0.display()
            ))
        })?;
        let entries: Vec<OutputEntry<Self::Handle>> = serde_json::from_str(&json)
            .map_err(|e| RplError::Hq(format!("failed to parse manifest: {e}")))?;
        let _ = fs::remove_file(&token.0);
        Ok(entries)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
        let col = Arc::new(Int32Array::from(vec![10, 20, 30]));
        RecordBatch::try_new(schema, vec![col]).unwrap()
    }

    #[test]
    fn roundtrip() {
        let transport = FileTransport::temp().unwrap();
        let batch = test_batch();

        let handle = transport.store(&batch).unwrap();
        assert!(handle.path().exists());

        let loaded = transport.load(&handle).unwrap();
        assert_eq!(loaded.num_rows(), 3);
        assert_eq!(
            loaded.column(0).as_any().downcast_ref::<Int32Array>().unwrap().values(),
            &[10, 20, 30],
        );

        transport.release(&handle).unwrap();
        assert!(!handle.path().exists());
    }

    #[test]
    fn multi_consumer_file() {
        let transport = FileTransport::temp().unwrap();
        let batch = test_batch();

        let handle = transport.store(&batch).unwrap();
        transport.add_consumers(&handle, 1).unwrap();

        transport.release(&handle).unwrap();
        assert!(handle.path().exists()); // still has one consumer

        transport.release(&handle).unwrap();
        assert!(!handle.path().exists()); // cleaned up
    }
}
