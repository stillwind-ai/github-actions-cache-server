//! Filesystem storage. The configured root is wholly owned by the server
//! (ADR-0001): every top-level entry is either an upload/storage-location
//! folder or a temp entry, and anything else is Orphaned Storage.

use std::io;
use std::path::{Component, Path, PathBuf};
use std::time::UNIX_EPOCH;

use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::Stream;

use super::backend::{
    FilesystemUsage, StorageDeletion, StorageError, StorageFolder, StorageObject,
};
use super::io::{ByteStream, FileIo, blocking};

#[derive(Clone)]
pub struct FsStorage {
    root: PathBuf,
    io: FileIo,
}

impl FsStorage {
    pub async fn new(root: impl AsRef<Path>, io: FileIo) -> io::Result<Self> {
        let root = root.as_ref().to_path_buf();
        tokio::fs::create_dir_all(&root).await?;
        let root = tokio::fs::canonicalize(&root).await?;
        Ok(Self { root, io })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

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
    pub async fn write<S>(
        &self,
        name: &str,
        stream: S,
        expected_len: Option<u64>,
    ) -> Result<u64, StorageError>
    where
        S: Stream<Item = io::Result<Bytes>> + Send + 'static,
    {
        let path = self.path(name)?;
        let temp = self.root.join(format!("tmp-{}", uuid::Uuid::new_v4()));

        let result = async {
            let written = self.io.write_file(temp.clone(), stream).await?;
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

    pub async fn exists(&self, name: &str) -> Result<bool, StorageError> {
        Ok(self.io.file_size(self.path(name)?).await?.is_some())
    }

    /// Recursively deletes a folder, reporting what it contained. A missing
    /// folder is an empty deletion.
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

    /// Inventory of every top-level entry under the root.
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
            && let Some(modified) = DateTime::<Utc>::from_timestamp(
                since_epoch.as_secs() as i64,
                since_epoch.subsec_nanos(),
            )
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
        assert!(storage.list_folder("missing").await.unwrap().is_empty());

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
    async fn reports_filesystem_usage() {
        let (_dir, storage) = storage().await;
        let usage = storage.filesystem_usage().await.unwrap();
        assert!(usage.capacity_bytes > 0 && usage.used_bytes <= usage.capacity_bytes);
    }
}
