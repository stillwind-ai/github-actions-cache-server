//! Filesystem storage. The configured root is wholly owned by the server
//! (ADR-0001): every top-level entry is either an upload/storage-location
//! folder or a temp entry, and anything else is Orphaned Storage.

use std::io;
use std::path::{Component, Path, PathBuf};
use std::time::UNIX_EPOCH;

use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::Stream;

use super::io::{ByteStream, FileIo, blocking};

#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("Object not found in storage: {0}")]
    NotFound(String),
    #[error("Invalid object name `{0}`")]
    InvalidName(String),
    #[error(transparent)]
    Io(#[from] io::Error),
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
pub struct FsStorage {
    root: PathBuf,
    io: FileIo,
}

impl FsStorage {
    /// # Errors
    ///
    /// If the storage root can't be created or resolved.
    pub async fn new(root: impl AsRef<Path>, io: FileIo) -> io::Result<Self> {
        let root = root.as_ref().to_path_buf();
        tokio::fs::create_dir_all(&root).await?;
        let root = tokio::fs::canonicalize(&root).await?;
        Ok(Self { root, io })
    }

    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
    }

    #[must_use]
    pub fn io(&self) -> &FileIo {
        &self.io
    }

    /// Resolves an object name (`folder/parts/0`) inside the root. Names are
    /// relative, slash-separated and may not escape the root.
    fn path(&self, name: &str) -> Result<PathBuf, StorageError> {
        let relative = Path::new(name);
        let valid = !name.is_empty()
            && relative
                .components()
                .all(|component| matches!(component, Component::Normal(_)));
        if !valid {
            return Err(StorageError::InvalidName(name.to_owned()));
        }
        Ok(self.root.join(relative))
    }

    /// Streams an object, failing with [`StorageError::NotFound`] up front.
    ///
    /// # Errors
    ///
    /// [`StorageError::NotFound`] when the object doesn't exist,
    /// [`StorageError::InvalidName`] for a name outside the storage root, or the
    /// underlying I/O error.
    pub async fn read(&self, name: &str) -> Result<ByteStream, StorageError> {
        match self.io.read_file(self.path(name)?).await {
            Ok(stream) => Ok(stream),
            Err(err) if err.kind() == io::ErrorKind::NotFound => {
                Err(StorageError::NotFound(name.to_owned()))
            }
            Err(err) => Err(err.into()),
        }
    }

    /// Writes an object with atomic visibility (ADR-0004): the data goes to a
    /// top-level temp entry that is renamed into place, so the object never
    /// exists partially and replacing it never disturbs readers holding the
    /// previous file open. Temp entries orphaned by a crash are unauthorized
    /// top-level storage, reclaimed by `cleanup:orphaned-storage` after the
    /// grace period; each write gets its own so none stays perpetually fresh.
    ///
    /// With `expected_len`, a stream of any other length fails the write before
    /// the object becomes visible.
    ///
    /// # Errors
    ///
    /// [`StorageError::InvalidName`] for a name outside the storage root, or the
    /// underlying I/O error. A length mismatch with `expected_len` is an
    /// [`io::ErrorKind::InvalidData`] error.
    pub async fn write<S>(
        &self,
        name: &str,
        stream: S,
        expected_len: Option<u64>,
    ) -> Result<u64, StorageError>
    where
        S: Stream<Item = io::Result<Bytes>> + Send + 'static,
    {
        self.write_atomically(name, expected_len, |temp| self.io.write_file(temp, stream))
            .await
    }

    /// Writes the concatenation of the `sources` objects as `name`, like
    /// [`Self::write`]. The data never enters user space: `copy_file_range`
    /// copies it inside the kernel, or shares the extents outright on
    /// filesystems with reflinks (XFS, btrfs).
    ///
    /// # Errors
    ///
    /// [`StorageError::InvalidName`] for a name outside the storage root, or the
    /// underlying I/O error: a missing source is [`io::ErrorKind::NotFound`], a
    /// length other than `expected_len` [`io::ErrorKind::InvalidData`].
    pub async fn concat(
        &self,
        sources: &[String],
        name: &str,
        expected_len: u64,
    ) -> Result<u64, StorageError> {
        let sources = sources
            .iter()
            .map(|source| self.path(source))
            .collect::<Result<Vec<_>, _>>()?;
        self.write_atomically(name, Some(expected_len), |temp| {
            blocking(move || {
                let mut target = std::fs::File::create(temp)?;
                let mut written = 0;
                for source in sources {
                    // `io::copy` between files is `copy_file_range`, with
                    // fallbacks for filesystems that refuse it.
                    written += std::io::copy(&mut std::fs::File::open(source)?, &mut target)?;
                }
                Ok(written)
            })
        })
        .await
    }

    /// Runs `write` against a fresh temp entry and renames it to `name` (see
    /// [`Self::write`]).
    async fn write_atomically<F, W>(
        &self,
        name: &str,
        expected_len: Option<u64>,
        write: F,
    ) -> Result<u64, StorageError>
    where
        F: FnOnce(PathBuf) -> W,
        W: Future<Output = io::Result<u64>>,
    {
        let path = self.path(name)?;
        let temp = self.root.join(format!("tmp-{}", uuid::Uuid::new_v4()));

        let result = async {
            let written = write(temp.clone()).await?;
            if let Some(expected) = expected_len
                && written != expected
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("wrote {written} bytes to {name}, expected {expected}"),
                ));
            }
            if let Some(parent) = path.parent() {
                self.io.create_dir_all(parent.to_path_buf()).await?;
            }
            self.io.rename(temp.clone(), path).await?;
            Ok::<_, io::Error>(written)
        }
        .await;

        if result.is_err()
            && let Err(err) = self.io.remove_file(temp.clone()).await
            && err.kind() != io::ErrorKind::NotFound
        {
            tracing::warn!(path = %temp.display(), error = %err, "Failed to remove temp file");
        }
        Ok(result?)
    }

    /// Atomically renames an object.
    ///
    /// # Errors
    ///
    /// [`StorageError::NotFound`] when `from` doesn't exist,
    /// [`StorageError::InvalidName`] for a name outside the storage root, or the
    /// underlying I/O error.
    pub async fn rename(&self, from: &str, to: &str) -> Result<(), StorageError> {
        match self.io.rename(self.path(from)?, self.path(to)?).await {
            Err(err) if err.kind() == io::ErrorKind::NotFound => {
                Err(StorageError::NotFound(from.to_owned()))
            }
            result => Ok(result?),
        }
    }

    /// # Errors
    ///
    /// [`StorageError::InvalidName`] for a name outside the storage root, or the
    /// underlying I/O error.
    pub async fn exists(&self, name: &str) -> Result<bool, StorageError> {
        Ok(self.io.file_size(self.path(name)?).await?.is_some())
    }

    /// Recursively deletes a folder, reporting what it contained. A missing
    /// folder is an empty deletion.
    ///
    /// # Errors
    ///
    /// [`StorageError::InvalidName`] for a name outside the storage root, or the
    /// underlying I/O error.
    pub async fn delete_folder(&self, folder: &str) -> Result<StorageDeletion, StorageError> {
        let path = self.path(folder)?;
        let folder = folder.to_owned();
        Ok(blocking(move || {
            let Some(inventory) = inspect(&path, folder)? else {
                return Ok(StorageDeletion::default());
            };
            match std::fs::remove_dir_all(&path) {
                Err(err) if err.kind() == io::ErrorKind::NotADirectory => {
                    std::fs::remove_file(&path)
                }
                result => result,
            }
            .or_else(|err| match err.kind() {
                io::ErrorKind::NotFound => Ok(()),
                _ => Err(err),
            })?;
            Ok(StorageDeletion {
                objects: inventory.object_count,
                bytes: inventory.bytes,
            })
        })
        .await?)
    }

    /// Files directly inside a folder, names relative to it. A missing folder
    /// is empty.
    ///
    /// # Errors
    ///
    /// [`StorageError::InvalidName`] for a name outside the storage root, or the
    /// underlying I/O error.
    pub async fn list_folder(&self, folder: &str) -> Result<Vec<StorageObject>, StorageError> {
        let path = self.path(folder)?;
        Ok(blocking(move || {
            let entries = match std::fs::read_dir(&path) {
                Ok(entries) => entries,
                Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
                Err(err) => return Err(err),
            };
            let mut objects = Vec::new();
            for entry in entries {
                let entry = entry?;
                let metadata = entry.metadata()?;
                if metadata.is_file() {
                    objects.push(StorageObject {
                        name: entry.file_name().to_string_lossy().into_owned(),
                        bytes: metadata.len(),
                    });
                }
            }
            Ok(objects)
        })
        .await?)
    }

    /// Number of files directly inside a folder, from the directory entries
    /// alone (no stat per file). A missing folder is empty.
    ///
    /// # Errors
    ///
    /// [`StorageError::InvalidName`] for a name outside the storage root, or the
    /// underlying I/O error.
    pub async fn count_files(&self, folder: &str) -> Result<usize, StorageError> {
        let path = self.path(folder)?;
        Ok(blocking(move || {
            let entries = match std::fs::read_dir(&path) {
                Ok(entries) => entries,
                Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(0),
                Err(err) => return Err(err),
            };
            let mut count = 0;
            for entry in entries {
                if entry?.file_type()?.is_file() {
                    count += 1;
                }
            }
            Ok(count)
        })
        .await?)
    }

    /// Inventory of every top-level entry under the root.
    ///
    /// # Errors
    ///
    /// If the storage root can't be listed.
    pub async fn list_storage_folders(&self) -> Result<Vec<StorageFolder>, StorageError> {
        let root = self.root.clone();
        Ok(blocking(move || {
            let mut folders = Vec::new();
            for entry in std::fs::read_dir(&root)? {
                let entry = entry?;
                let name = entry.file_name().to_string_lossy().into_owned();
                if let Some(folder) = inspect(&entry.path(), name)? {
                    folders.push(folder);
                }
            }
            Ok(folders)
        })
        .await?)
    }

    /// Capacity and occupancy of the volume holding the root (Filesystem
    /// Capacity), including data outside the cache directory.
    ///
    /// # Errors
    ///
    /// If the filesystem statistics can't be read.
    pub async fn filesystem_usage(&self) -> Result<FilesystemUsage, StorageError> {
        let root = self.root.clone();
        Ok(blocking(move || {
            let stat = rustix::fs::statvfs(&root).map_err(io::Error::from)?;
            Ok(FilesystemUsage {
                capacity_bytes: stat.f_blocks * stat.f_frsize,
                used_bytes: (stat.f_blocks - stat.f_bavail) * stat.f_frsize,
            })
        })
        .await?)
    }
}

/// Walks an entry without following symlinks. `None` if it doesn't exist.
fn inspect(path: &Path, folder_name: String) -> io::Result<Option<StorageFolder>> {
    let mut folder = StorageFolder {
        folder_name,
        object_count: 0,
        bytes: 0,
        updated_at: DateTime::<Utc>::UNIX_EPOCH,
    };
    let mut pending = vec![path.to_path_buf()];
    let mut found = false;
    while let Some(current) = pending.pop() {
        let metadata = match std::fs::symlink_metadata(&current) {
            Ok(metadata) => metadata,
            Err(err) if err.kind() == io::ErrorKind::NotFound => continue,
            Err(err) => return Err(err),
        };
        found = true;
        if let Ok(modified) = metadata.modified()
            && let Ok(since_epoch) = modified.duration_since(UNIX_EPOCH)
            && let Ok(secs) = i64::try_from(since_epoch.as_secs())
            && let Some(modified) =
                DateTime::<Utc>::from_timestamp(secs, since_epoch.subsec_nanos())
        {
            folder.updated_at = folder.updated_at.max(modified);
        }
        if metadata.file_type().is_symlink() {
            return Err(io::Error::other(format!(
                "Refusing to inspect symbolic link in owned storage: {}",
                current.display()
            )));
        }
        if metadata.is_dir() {
            match std::fs::read_dir(&current) {
                Ok(entries) => {
                    for entry in entries {
                        pending.push(entry?.path());
                    }
                }
                Err(err) if err.kind() == io::ErrorKind::NotFound => {}
                Err(err) => return Err(err),
            }
        } else {
            folder.object_count += 1;
            folder.bytes += metadata.len();
        }
    }
    Ok(found.then_some(folder))
}

#[cfg(test)]
mod tests {
    use futures::StreamExt;

    use super::*;

    async fn storage() -> (tempfile::TempDir, FsStorage) {
        let dir = tempfile::tempdir().unwrap();
        let storage = FsStorage::new(dir.path().join("root"), FileIo::Tokio)
            .await
            .unwrap();
        (dir, storage)
    }

    fn body(data: &'static [u8]) -> impl Stream<Item = io::Result<Bytes>> + Send + 'static {
        futures::stream::iter([Ok(Bytes::from_static(data))])
    }

    #[tokio::test]
    async fn rejects_names_escaping_the_root() {
        let (_dir, storage) = storage().await;
        for name in ["", "../x", "a/../../x", "/etc/passwd", "./x"] {
            assert!(
                matches!(
                    storage.exists(name).await,
                    Err(StorageError::InvalidName(_))
                ),
                "{name}"
            );
        }
    }

    #[tokio::test]
    async fn writes_atomically_and_inventories_folders() {
        let (_dir, storage) = storage().await;
        assert_eq!(
            storage
                .write("123/parts/0", body(b"hello"), Some(5))
                .await
                .unwrap(),
            5
        );
        assert_eq!(
            storage
                .write("123/parts/1", body(b"world!"), None)
                .await
                .unwrap(),
            6
        );

        let mut parts = storage.list_folder("123/parts").await.unwrap();
        parts.sort_by(|a, b| a.name.cmp(&b.name));
        assert_eq!(
            parts,
            [
                StorageObject {
                    name: "0".into(),
                    bytes: 5
                },
                StorageObject {
                    name: "1".into(),
                    bytes: 6
                }
            ]
        );
        assert_eq!(storage.list_folder("missing").await.unwrap(), []);
        assert_eq!(storage.count_files("123/parts").await.unwrap(), 2);
        assert_eq!(storage.count_files("123").await.unwrap(), 0);
        assert_eq!(storage.count_files("missing").await.unwrap(), 0);

        // No temp entries are left behind.
        let folders = storage.list_storage_folders().await.unwrap();
        assert_eq!(folders.len(), 1);
        assert_eq!((folders[0].object_count, folders[0].bytes), (2, 11));

        let mut stream = storage.read("123/parts/1").await.unwrap();
        assert_eq!(
            stream.next().await.unwrap().unwrap(),
            Bytes::from_static(b"world!")
        );
        assert!(matches!(
            storage.read("123/merged").await,
            Err(StorageError::NotFound(_))
        ));

        assert_eq!(
            storage.delete_folder("123").await.unwrap(),
            StorageDeletion {
                objects: 2,
                bytes: 11
            }
        );
        assert_eq!(
            storage.delete_folder("123").await.unwrap(),
            StorageDeletion::default()
        );
        assert!(storage.list_storage_folders().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn failed_writes_leave_no_object() {
        let (_dir, storage) = storage().await;
        let failing =
            futures::stream::iter([Ok(Bytes::from_static(b"x")), Err(io::Error::other("boom"))]);
        assert!(storage.write("1/parts/0", failing, None).await.is_err());
        assert!(
            storage
                .write("1/merged", body(b"short"), Some(6))
                .await
                .is_err()
        );
        assert!(!storage.exists("1/merged").await.unwrap());
        assert!(!storage.exists("1/parts/0").await.unwrap());
        assert!(storage.list_storage_folders().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn concatenates_objects() {
        let (_dir, storage) = storage().await;
        storage
            .write("7/parts/0", body(b"hello "), None)
            .await
            .unwrap();
        storage
            .write("7/parts/1", body(b"world"), None)
            .await
            .unwrap();
        let parts = ["7/parts/0".to_owned(), "7/parts/1".to_owned()];
        assert_eq!(storage.concat(&parts, "7/merged", 11).await.unwrap(), 11);
        let mut merged = Vec::new();
        let mut stream = storage.read("7/merged").await.unwrap();
        while let Some(chunk) = stream.next().await {
            merged.extend_from_slice(&chunk.unwrap());
        }
        assert_eq!(merged, b"hello world");

        // A length mismatch or a missing source leaves no object behind.
        assert!(storage.concat(&parts, "8/merged", 12).await.is_err());
        let missing = ["7/parts/0".to_owned(), "7/parts/2".to_owned()];
        assert!(storage.concat(&missing, "9/merged", 6).await.is_err());
        assert!(!storage.exists("8/merged").await.unwrap());
        assert!(!storage.exists("9/merged").await.unwrap());
        assert_eq!(storage.list_storage_folders().await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn reports_filesystem_usage() {
        let (_dir, storage) = storage().await;
        let usage = storage.filesystem_usage().await.unwrap();
        assert!(usage.capacity_bytes > 0 && usage.used_bytes <= usage.capacity_bytes);
    }
}
