//! The storage backends behind the cache storage engine: filesystem storage
//! or an S3 bucket. Both expose the same object model — slash-separated
//! object names grouped into top-level folders — with atomically visible
//! writes (ADR-0004).

use std::io;
use std::time::Duration;

use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::Stream;

use super::fs::FsStorage;
use super::io::ByteStream;
use super::s3::S3Storage;

#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("Object not found in storage: {0}")]
    NotFound(String),
    #[error("Invalid object name `{0}`")]
    InvalidName(String),
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error("S3 request failed: {0}")]
    S3(String),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StorageObject {
    pub name: String,
    pub bytes: u64,
}

#[derive(Clone, Debug)]
pub struct StorageFolder {
    pub folder_name: String,
    pub object_count: u64,
    pub bytes: u64,
    /// Newest modification time of the folder or anything inside it.
    pub updated_at: DateTime<Utc>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StorageDeletion {
    pub objects: u64,
    pub bytes: u64,
}

#[derive(Clone, Copy, Debug)]
pub struct FilesystemUsage {
    pub capacity_bytes: u64,
    pub used_bytes: u64,
}

/// Limits a set of Parts must satisfy for a Server-side Merge (ADR-0009).
#[derive(Clone, Copy, Debug)]
pub struct ComposeLimits {
    /// Every Part but the last must be at least this large.
    pub min_part_bytes: u64,
    pub max_part_bytes: u64,
    pub max_parts: usize,
}

impl ComposeLimits {
    /// Whether Parts of these sizes, in order, can be composed.
    pub fn allows(&self, part_sizes: &[u64]) -> bool {
        !part_sizes.is_empty()
            && part_sizes.len() <= self.max_parts
            && part_sizes.iter().enumerate().all(|(index, &bytes)| {
                bytes <= self.max_part_bytes
                    && (index == part_sizes.len() - 1 || bytes >= self.min_part_bytes)
            })
    }
}

#[derive(Clone)]
pub enum Backend {
    Filesystem(FsStorage),
    S3(S3Storage),
}

impl Backend {
    pub fn name(&self) -> &'static str {
        match self {
            Self::Filesystem(_) => "filesystem",
            Self::S3(_) => "s3",
        }
    }

    /// Streams an object, failing with [`StorageError::NotFound`] up front.
    pub async fn read(&self, name: &str) -> Result<ByteStream, StorageError> {
        match self {
            Self::Filesystem(fs) => fs.read(name).await,
            Self::S3(s3) => s3.read(name).await,
        }
    }

    /// Writes an object with atomic visibility (ADR-0004). With
    /// `expected_len`, a stream of any other length fails the write before
    /// the object becomes visible.
    pub async fn write<S>(
        &self,
        name: &str,
        stream: S,
        expected_len: Option<u64>,
    ) -> Result<u64, StorageError>
    where
        S: Stream<Item = io::Result<Bytes>> + Send + 'static,
    {
        match self {
            Self::Filesystem(fs) => fs.write(name, stream, expected_len).await,
            Self::S3(s3) => s3.write(name, stream, expected_len).await,
        }
    }

    pub async fn exists(&self, name: &str) -> Result<bool, StorageError> {
        match self {
            Self::Filesystem(fs) => fs.exists(name).await,
            Self::S3(s3) => s3.exists(name).await,
        }
    }

    /// Deletes everything inside a folder, reporting what it contained. A
    /// missing folder is an empty deletion.
    pub async fn delete_folder(&self, folder: &str) -> Result<StorageDeletion, StorageError> {
        match self {
            Self::Filesystem(fs) => fs.delete_folder(folder).await,
            Self::S3(s3) => s3.delete_folder(folder).await,
        }
    }

    /// Objects directly inside a folder, names relative to it. A missing
    /// folder is empty.
    pub async fn list_folder(&self, folder: &str) -> Result<Vec<StorageObject>, StorageError> {
        match self {
            Self::Filesystem(fs) => fs.list_folder(folder).await,
            Self::S3(s3) => s3.list_folder(folder).await,
        }
    }

    pub async fn count_files(&self, folder: &str) -> Result<usize, StorageError> {
        Ok(self.list_folder(folder).await?.len())
    }

    /// Inventory of every top-level folder the server owns.
    pub async fn list_storage_folders(&self) -> Result<Vec<StorageFolder>, StorageError> {
        match self {
            Self::Filesystem(fs) => fs.list_storage_folders().await,
            Self::S3(s3) => s3.list_storage_folders().await,
        }
    }

    /// Filesystem Capacity, for a Storage Budget relative to it. `None` for
    /// object storage, which has no capacity to speak of.
    pub async fn filesystem_usage(&self) -> Result<Option<FilesystemUsage>, StorageError> {
        match self {
            Self::Filesystem(fs) => fs.filesystem_usage().await.map(Some),
            Self::S3(_) => Ok(None),
        }
    }

    /// The limits of a Server-side Merge, when the backend has one.
    pub fn compose_limits(&self) -> Option<ComposeLimits> {
        match self {
            Self::Filesystem(_) => None,
            Self::S3(_) => Some(S3Storage::COMPOSE_LIMITS),
        }
    }

    /// Server-side Merge: copies `parts/0..n-1` of a folder into its merged
    /// object inside the backend, without passing bytes through the server.
    /// The caller checks [`Self::compose_limits`] first.
    pub async fn compose_parts(&self, folder: &str, part_count: u32) -> Result<(), StorageError> {
        match self {
            Self::Filesystem(_) => Err(StorageError::Io(io::Error::new(
                io::ErrorKind::Unsupported,
                "filesystem storage has no Server-side Merge",
            ))),
            Self::S3(s3) => s3.compose_parts(folder, part_count).await,
        }
    }

    /// Whether the backend can hand out direct-download URLs.
    pub fn supports_direct_downloads(&self) -> bool {
        matches!(self, Self::S3(_))
    }

    /// A URL that downloads an object directly from the backend, valid for
    /// `expires_in`. `None` when the backend has no such URLs.
    pub async fn download_url(
        &self,
        name: &str,
        expires_in: Duration,
    ) -> Result<Option<String>, StorageError> {
        match self {
            Self::Filesystem(_) => Ok(None),
            Self::S3(s3) => s3.download_url(name, expires_in).await.map(Some),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compose_limits_apply_to_all_parts_but_the_last() {
        let limits = ComposeLimits {
            min_part_bytes: 5,
            max_part_bytes: 10,
            max_parts: 3,
        };
        assert!(limits.allows(&[5, 10, 1]));
        assert!(limits.allows(&[1]));
        assert!(!limits.allows(&[]));
        assert!(!limits.allows(&[4, 5]), "a non-final part is too small");
        assert!(!limits.allows(&[5, 11]), "a part is too large");
        assert!(!limits.allows(&[5, 5, 5, 5]), "too many parts");
    }
}
