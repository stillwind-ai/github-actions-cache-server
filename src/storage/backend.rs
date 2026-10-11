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

#[derive(Clone)]
pub enum Backend {
    Filesystem(FsStorage),
    S3(S3Storage),
}

// The S3 arms are boxed: the AWS SDK's futures are large, and inlining them
// here would bloat every future that awaits storage.
impl Backend {
    #[must_use]
    pub fn name(&self) -> &'static str {
        match self {
            Self::Filesystem(_) => "filesystem",
            Self::S3(_) => "s3",
        }
    }

    /// Streams an object.
    ///
    /// # Errors
    ///
    /// [`StorageError::NotFound`] up front for a missing object,
    /// [`StorageError::InvalidName`], or the backend's error.
    pub async fn read(&self, name: &str) -> Result<ByteStream, StorageError> {
        match self {
            Self::Filesystem(fs) => fs.read(name).await,
            Self::S3(s3) => Box::pin(s3.read(name)).await,
        }
    }

    /// Writes an object with atomic visibility (ADR-0004). With
    /// `expected_len`, a stream of any other length fails the write before
    /// the object becomes visible.
    ///
    /// # Errors
    ///
    /// [`StorageError::InvalidName`], a stream error or length mismatch, or
    /// the backend's error; the object is then left as it was.
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
            Self::S3(s3) => Box::pin(s3.write(name, stream, expected_len)).await,
        }
    }

    /// Whether an object exists.
    ///
    /// # Errors
    ///
    /// [`StorageError::InvalidName`] or the backend's error.
    pub async fn exists(&self, name: &str) -> Result<bool, StorageError> {
        match self {
            Self::Filesystem(fs) => fs.exists(name).await,
            Self::S3(s3) => Box::pin(s3.exists(name)).await,
        }
    }

    /// Deletes everything inside a folder, reporting what it contained. A
    /// missing folder is an empty deletion.
    ///
    /// # Errors
    ///
    /// [`StorageError::InvalidName`] or the backend's error.
    pub async fn delete_folder(&self, folder: &str) -> Result<StorageDeletion, StorageError> {
        match self {
            Self::Filesystem(fs) => fs.delete_folder(folder).await,
            Self::S3(s3) => Box::pin(s3.delete_folder(folder)).await,
        }
    }

    /// Objects directly inside a folder, names relative to it. A missing
    /// folder is empty.
    ///
    /// # Errors
    ///
    /// [`StorageError::InvalidName`] or the backend's error.
    pub async fn list_folder(&self, folder: &str) -> Result<Vec<StorageObject>, StorageError> {
        match self {
            Self::Filesystem(fs) => fs.list_folder(folder).await,
            Self::S3(s3) => Box::pin(s3.list_folder(folder)).await,
        }
    }

    /// Number of objects directly inside a folder.
    ///
    /// # Errors
    ///
    /// As for [`Self::list_folder`].
    pub async fn count_files(&self, folder: &str) -> Result<usize, StorageError> {
        match self {
            Self::Filesystem(fs) => fs.count_files(folder).await,
            Self::S3(s3) => Ok(Box::pin(s3.list_folder(folder)).await?.len()),
        }
    }

    /// Inventory of every top-level folder the server owns.
    ///
    /// # Errors
    ///
    /// If the storage can't be listed completely.
    pub async fn list_storage_folders(&self) -> Result<Vec<StorageFolder>, StorageError> {
        match self {
            Self::Filesystem(fs) => fs.list_storage_folders().await,
            Self::S3(s3) => Box::pin(s3.list_storage_folders()).await,
        }
    }

    /// Filesystem Capacity, for a Storage Budget relative to it. `None` for
    /// object storage, which has no capacity to speak of.
    ///
    /// # Errors
    ///
    /// If the filesystem's usage can't be read.
    pub async fn filesystem_usage(&self) -> Result<Option<FilesystemUsage>, StorageError> {
        match self {
            Self::Filesystem(fs) => fs.filesystem_usage().await.map(Some),
            Self::S3(_) => Ok(None),
        }
    }

    /// Writes the concatenation of the `sources` objects as `name`, with
    /// atomic visibility and the length checked against `expected_len`
    /// before it becomes visible. This is the Merge: on the filesystem the
    /// data never enters user space (`copy_file_range`, ADR-0011); on S3 it
    /// is a Server-side Merge when the sources meet S3's multipart limits and
    /// streamed through the server otherwise (ADR-0009).
    ///
    /// # Errors
    ///
    /// [`StorageError::InvalidName`], [`StorageError::NotFound`] or an
    /// [`io::ErrorKind::NotFound`] error for a missing source, an
    /// [`io::ErrorKind::InvalidData`] error for a length other than
    /// `expected_len`, or the backend's error; the object is then left as it
    /// was.
    pub async fn concat(
        &self,
        sources: &[String],
        name: &str,
        expected_len: u64,
    ) -> Result<u64, StorageError> {
        match self {
            Self::Filesystem(fs) => fs.concat(sources, name, expected_len).await,
            Self::S3(s3) => Box::pin(s3.concat(sources, name, expected_len)).await,
        }
    }

    /// Whether [`Self::rename`] is available: an atomic, metadata-only rename.
    #[must_use]
    pub fn supports_rename(&self) -> bool {
        matches!(self, Self::Filesystem(_))
    }

    /// Atomically renames an object, where [`Self::supports_rename`].
    ///
    /// # Errors
    ///
    /// [`StorageError::NotFound`] when `from` doesn't exist,
    /// [`StorageError::InvalidName`], an [`io::ErrorKind::Unsupported`] error
    /// on object storage, or the underlying I/O error.
    pub async fn rename(&self, from: &str, to: &str) -> Result<(), StorageError> {
        match self {
            Self::Filesystem(fs) => fs.rename(from, to).await,
            Self::S3(_) => Err(StorageError::Io(io::Error::new(
                io::ErrorKind::Unsupported,
                "S3 storage has no atomic rename",
            ))),
        }
    }

    /// Whether the backend can hand out direct-download URLs.
    #[must_use]
    pub fn supports_direct_downloads(&self) -> bool {
        matches!(self, Self::S3(_))
    }

    /// A URL that downloads an object directly from the backend, valid for
    /// `expires_in`. `None` when the backend has no such URLs.
    ///
    /// # Errors
    ///
    /// [`StorageError::InvalidName`] or if the URL can't be signed.
    pub async fn download_url(
        &self,
        name: &str,
        expires_in: Duration,
    ) -> Result<Option<String>, StorageError> {
        match self {
            Self::Filesystem(_) => Ok(None),
            Self::S3(s3) => Box::pin(s3.download_url(name, expires_in)).await.map(Some),
        }
    }
}
