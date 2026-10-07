//! The cache storage engine: Uploads, Cache Entries, Storage Locations and
//! their Merge, on filesystem storage with Postgres as the source of truth.

pub mod fs;
pub mod io;
pub mod leases;
pub mod lifecycle;

use std::collections::BTreeSet;
use std::sync::Arc;

use bytes::Bytes;
use chrono::Utc;
use futures::{Stream, StreamExt};
use rand::Rng;
use sea_orm::sea_query::{Expr, Func, LikeExpr, LockType, OnConflict, Query};
use sea_orm::{
    ActiveValue::Set, ColumnTrait, DatabaseConnection, DbErr, EntityTrait, ExprTrait, JoinType,
    Order, QueryFilter, QueryOrder, QuerySelect, QueryTrait, RelationTrait, TransactionTrait,
};
use serde::Serialize;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::task::TaskTracker;
use uuid::Uuid;

use self::fs::{FsStorage, StorageError};
use self::io::ByteStream;
use self::leases::LEASE_RENEWAL;
use self::lifecycle::{delete_storage_location_if_unread, no_active_reader_lease};
use crate::config::Config;
use crate::db::retry_on_lock_conflict;
use crate::entity::storage_reader_lease::ReaderScope;
use crate::entity::{cache_entry, merge_lease, storage_location, upload};

/// Bounds the self-heal retry when matching keeps surfacing Dangling Cache
/// Entries for the same prefix (ADR-0005).
const MAX_DANGLING_PURGE_ATTEMPTS: usize = 10;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Db(#[from] DbErr),
    #[error(transparent)]
    Storage(#[from] StorageError),
    /// The upload cannot become a Cache Entry; it has been abandoned.
    #[error("{0}")]
    UploadRejected(String),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum MatchType {
    ExactPrimary,
    PrefixedPrimary,
    ExactRestore,
    PrefixedRestore,
}

#[derive(Clone, Debug)]
pub struct CacheMatch {
    pub entry: cache_entry::Model,
    pub match_type: MatchType,
}

pub struct MatchQuery<'a> {
    pub primary_key: &'a str,
    pub restore_keys: &'a [String],
    pub version: &'a str,
    /// Checked in order.
    pub scopes: &'a [String],
    pub repo_id: &'a str,
}

pub struct Download {
    pub size: u64,
    pub stream: ByteStream,
}

#[derive(Clone, Copy, Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EvictionSummary {
    pub evicted_locations: u64,
    pub evicted_bytes: u64,
}

pub struct Storage {
    db: DatabaseConnection,
    fs: FsStorage,
    config: Arc<Config>,
    merges: TaskTracker,
}

fn parts_folder(folder_name: &str) -> String {
    format!("{folder_name}/parts")
}

fn part_name(folder_name: &str, index: u32) -> String {
    format!("{folder_name}/parts/{index}")
}

fn merged_name(folder_name: &str) -> String {
    format!("{folder_name}/merged")
}

fn escape_like(value: &str) -> String {
    value
        .replace('\\', r"\\")
        .replace('%', r"\%")
        .replace('_', r"\_")
}

/// Streams the Parts of a Storage Location in order, opening each lazily.
fn stream_parts(fs: FsStorage, folder_name: String, part_count: i32) -> ByteStream {
    Box::pin(async_stream::stream! {
        for index in 0..part_count as u32 {
            let mut part = match fs.read(&part_name(&folder_name, index)).await {
                Ok(part) => part,
                Err(err) => {
                    yield Err(std::io::Error::other(err));
                    return;
                }
            };
            while let Some(chunk) = part.next().await {
                let failed = chunk.is_err();
                yield chunk;
                if failed {
                    return;
                }
            }
        }
    })
}

impl Storage {
    pub fn new(db: DatabaseConnection, fs: FsStorage, config: Arc<Config>) -> Self {
        Self {
            db,
            fs,
            config,
            merges: TaskTracker::new(),
        }
    }

    pub fn fs(&self) -> &FsStorage {
        &self.fs
    }

    pub fn db(&self) -> &DatabaseConnection {
        &self.db
    }

    /// Waits for background merges, e.g. before shutting down.
    pub async fn wait_for_ongoing_merges(&self) {
        self.merges.close();
        self.merges.wait().await;
        self.merges.reopen();
    }

    /// Starts an Upload. `None` when an Upload for the same key is already in
    /// progress.
    pub async fn create_upload(
        &self,
        key: &str,
        version: &str,
        scope: &str,
        repo_id: &str,
    ) -> Result<Option<i64>> {
        // The id doubles as the unauthenticated upload URL's secret, so it is
        // random rather than sequential (kept within JavaScript's safe range).
        let id = rand::rng().random_range(1..(1_i64 << 53));
        let inserted = upload::Entity::insert(upload::ActiveModel {
            id: Set(id),
            repo_id: Set(repo_id.to_owned()),
            scope: Set(scope.to_owned()),
            version: Set(version.to_owned()),
            key: Set(key.to_owned()),
            folder_name: Set(id.to_string()),
            created_at: Set(Utc::now()),
            last_part_uploaded_at: Set(None),
            started_part_upload_count: Set(0),
            finished_part_upload_count: Set(0),
        })
        .on_conflict_do_nothing_on([
            upload::Column::RepoId,
            upload::Column::Scope,
            upload::Column::Version,
            upload::Column::Key,
        ])
        .exec_without_returning(&self.db)
        .await?;
        Ok(match inserted {
            sea_orm::TryInsertResult::Inserted(1) => Some(id),
            _ => None,
        })
    }

    /// Stores one Part of an Upload. False when the Upload does not exist.
    pub async fn upload_part<S>(&self, upload_id: i64, index: u32, body: S) -> Result<bool>
    where
        S: Stream<Item = std::io::Result<Bytes>> + Send + 'static,
    {
        let Some(folder_name) = upload::Entity::find_by_id(upload_id)
            .select_only()
            .column(upload::Column::FolderName)
            .into_tuple::<String>()
            .one(&self.db)
            .await?
        else {
            return Ok(false);
        };

        upload::Entity::update_many()
            .col_expr(
                upload::Column::StartedPartUploadCount,
                Expr::col(upload::Column::StartedPartUploadCount).add(1),
            )
            .filter(upload::Column::Id.eq(upload_id))
            .exec(&self.db)
            .await?;

        self.fs
            .write(&part_name(&folder_name, index), body, None)
            .await?;

        upload::Entity::update_many()
            .col_expr(
                upload::Column::FinishedPartUploadCount,
                Expr::col(upload::Column::FinishedPartUploadCount).add(1),
            )
            .col_expr(upload::Column::LastPartUploadedAt, Expr::value(Utc::now()))
            .filter(upload::Column::Id.eq(upload_id))
            .exec(&self.db)
            .await?;
        Ok(true)
    }

    /// Database state first, then storage (ADR-0001).
    async fn abandon_upload(&self, upload: &upload::Model, reason: String) -> Error {
        if let Err(err) = upload::Entity::delete_by_id(upload.id).exec(&self.db).await {
            return err.into();
        }
        if let Err(err) = self.fs.delete_folder(&upload.folder_name).await {
            tracing::warn!(upload = upload.id, error = %err, "Failed to delete abandoned upload");
        }
        Error::UploadRejected(reason)
    }

    /// Turns a finished Upload into a Cache Entry, replacing an existing entry
    /// for the same key. Returns the Upload's id, or `None` if there is no
    /// such Upload.
    pub async fn complete_upload(
        &self,
        key: &str,
        version: &str,
        scope: &str,
        repo_id: &str,
    ) -> Result<Option<i64>> {
        let Some(upload) = upload::Entity::find()
            .filter(upload::Column::RepoId.eq(repo_id))
            .filter(upload::Column::Scope.eq(scope))
            .filter(upload::Column::Version.eq(version))
            .filter(upload::Column::Key.eq(key))
            .one(&self.db)
            .await?
        else {
            return Ok(None);
        };

        if upload.finished_part_upload_count == 0 {
            return Err(self
                .abandon_upload(&upload, "No parts have been uploaded".into())
                .await);
        }
        if upload.started_part_upload_count != upload.finished_part_upload_count {
            let reason = format!(
                "Not all parts have been uploaded (only {} of {} parts uploaded)",
                upload.finished_part_upload_count, upload.started_part_upload_count
            );
            return Err(self.abandon_upload(&upload, reason).await);
        }

        let parts = self
            .fs
            .list_folder(&parts_folder(&upload.folder_name))
            .await?;
        if parts.len() != upload.finished_part_upload_count as usize {
            let reason = format!(
                "Uploaded part count does not match actual part count in storage (expected {} but found {})",
                upload.finished_part_upload_count,
                parts.len()
            );
            return Err(self.abandon_upload(&upload, reason).await);
        }
        // Downloads read parts 0..n-1, so anything else would be a broken entry.
        let indices: BTreeSet<u32> = parts
            .iter()
            .filter_map(|part| part.name.parse().ok())
            .collect();
        if !indices.iter().copied().eq(0..parts.len() as u32) {
            return Err(self
                .abandon_upload(
                    &upload,
                    "Uploaded parts are not numbered contiguously from 0".into(),
                )
                .await);
        }

        let now = Utc::now();
        let location = storage_location::ActiveModel {
            id: Set(Uuid::new_v4()),
            folder_name: Set(upload.folder_name.clone()),
            part_count: Set(parts.len() as i32),
            size_bytes: Set(parts.iter().map(|part| part.bytes as i64).sum()),
            created_at: Set(now),
            merge_started_at: Set(None),
            merged_at: Set(None),
            parts_deleted_at: Set(None),
            last_downloaded_at: Set(None),
        };

        let txn = self.db.begin().await?;
        // Claim the Upload first: of concurrent finalizations (a retrying
        // client), exactly one may turn its folder into a Storage Location —
        // two locations sharing a folder would let cleanup of the unreferenced
        // one delete the live entry's data.
        let claimed = upload::Entity::delete_by_id(upload.id).exec(&txn).await?;
        if claimed.rows_affected != 1 {
            return Ok(None);
        }
        let location = storage_location::Entity::insert(location)
            .exec_with_returning(&txn)
            .await?;
        // The replaced entry's Storage Location is left unreferenced, for
        // `cleanup:storage-locations` to reclaim.
        cache_entry::Entity::insert(cache_entry::ActiveModel {
            id: Set(Uuid::new_v4()),
            repo_id: Set(repo_id.to_owned()),
            scope: Set(scope.to_owned()),
            version: Set(version.to_owned()),
            key: Set(key.to_owned()),
            updated_at: Set(now),
            location_id: Set(location.id),
        })
        .on_conflict(
            OnConflict::columns([
                cache_entry::Column::RepoId,
                cache_entry::Column::Scope,
                cache_entry::Column::Version,
                cache_entry::Column::Key,
            ])
            .update_columns([
                cache_entry::Column::UpdatedAt,
                cache_entry::Column::LocationId,
            ])
            .to_owned(),
        )
        .exec_without_returning(&txn)
        .await?;
        txn.commit().await?;

        match self.enforce_storage_budget().await {
            Ok(summary) if summary.evicted_locations > 0 => {
                tracing::info!(?summary, "Capacity-based Eviction reclaimed storage");
            }
            Ok(_) => {}
            Err(err) => {
                tracing::warn!(error = %err, "Capacity-based Eviction failed after upload completion");
            }
        }

        if self.config.eager_merge {
            // Filesystem storage has no Server-side Merge, so an Eager Merge
            // always streams the Parts (ADR-0009).
            let fs = self.fs.clone();
            let folder_name = location.folder_name.clone();
            let size = location.size_bytes as u64;
            let parts = stream_parts(fs.clone(), folder_name.clone(), location.part_count);
            let write = async move {
                fs.write(&merged_name(&folder_name), parts, Some(size))
                    .await
            };
            if let Err(err) = self.start_merge(&location, write).await {
                tracing::warn!(error = %err, "Eager Merge failed to start after upload completion");
            }
        }

        Ok(Some(upload.id))
    }

    async fn stored_bytes(&self) -> Result<u64> {
        let bytes = storage_location::Entity::find()
            .select_only()
            .expr(Expr::cust("COALESCE(SUM(size_bytes), 0)::bigint"))
            .into_tuple::<i64>()
            .one(&self.db)
            .await?
            .unwrap_or(0);
        Ok(u64::try_from(bytes).unwrap_or(0))
    }

    /// Total bytes of finalized payloads (the `cache_storage_bytes` metric).
    pub async fn total_stored_bytes(&self) -> Result<u64> {
        self.stored_bytes().await
    }

    /// Capacity-based Eviction (ADR-0008): once usage exceeds the Storage
    /// Budget, deletes Cache Entries in Cache Recency order until usage is at
    /// most 90% of it. Reader leases still protect locations being read.
    pub async fn enforce_storage_budget(&self) -> Result<EvictionSummary> {
        let mut summary = EvictionSummary::default();
        let filesystem_usage = match self.config.cache_max_size_bytes {
            Some(_) => None,
            None => Some(self.fs.filesystem_usage().await?),
        };
        let budget = match (self.config.cache_max_size_bytes, filesystem_usage) {
            (Some(max), _) => max,
            (None, Some(usage)) => (usage.capacity_bytes as f64
                * self.config.cache_filesystem_max_usage_percent
                / 100.0)
                .floor() as u64,
            (None, None) => unreachable!("filesystem usage is read without an explicit budget"),
        };
        let target = budget / 10 * 9 + budget % 10 * 9 / 10;
        let mut usage = match filesystem_usage {
            Some(usage) => usage.used_bytes,
            None => self.stored_bytes().await?,
        };
        if usage <= budget {
            return Ok(summary);
        }

        let recency = Func::coalesce([
            Expr::col((
                storage_location::Entity,
                storage_location::Column::LastDownloadedAt,
            )),
            Expr::col((cache_entry::Entity, cache_entry::Column::UpdatedAt)),
            Expr::col((
                storage_location::Entity,
                storage_location::Column::CreatedAt,
            )),
        ]);
        let candidates = storage_location::Entity::find()
            .select_only()
            .columns([
                storage_location::Column::Id,
                storage_location::Column::FolderName,
                storage_location::Column::SizeBytes,
            ])
            .join(
                JoinType::LeftJoin,
                storage_location::Relation::CacheEntry.def(),
            )
            .filter(no_active_reader_lease(None))
            .order_by(recency, Order::Asc)
            .into_tuple::<(Uuid, String, i64)>()
            .all(&self.db)
            .await?;

        for (id, folder_name, size_bytes) in candidates {
            if usage <= target {
                break;
            }
            let txn = self.db.begin().await?;
            let deleted = delete_storage_location_if_unread(&txn, id).await?;
            txn.commit().await?;
            if !deleted {
                continue;
            }
            let reclaimed = self.fs.delete_folder(&folder_name).await?;
            summary.evicted_locations += 1;
            summary.evicted_bytes += reclaimed.bytes;
            usage = match filesystem_usage {
                Some(_) => self.fs.filesystem_usage().await?.used_bytes,
                None => usage.saturating_sub(size_bytes as u64),
            };
        }
        Ok(summary)
    }

    /// Starts a proxied download, taking a Storage Reader Lease for its
    /// lifetime. The first download of an unmerged entry also performs the
    /// Merge, feeding the same Part bytes to the client and the merged object.
    /// `None` when the entry doesn't exist or its data is gone.
    pub async fn download(&self, cache_entry_id: Uuid) -> Result<Option<Download>> {
        // The reader's lease scope is chosen in the same locked transaction
        // that reads the merge state (ADR-0002).
        let txn = self.db.begin().await?;
        let mut query = storage_location::Entity::find()
            .join(
                JoinType::InnerJoin,
                storage_location::Relation::CacheEntry.def(),
            )
            .filter(cache_entry::Column::Id.eq(cache_entry_id));
        QueryTrait::query(&mut query)
            .lock_with_tables(LockType::Update, [storage_location::Entity]);
        let Some(location) = query.one(&txn).await? else {
            return Ok(None);
        };
        let scope = if location.merged_at.is_some() {
            ReaderScope::Storage
        } else {
            ReaderScope::Parts
        };
        let lease_id = leases::create_reader_lease(&txn, location.id, scope).await?;
        txn.commit().await?;

        // A Cache Access, for Cache Recency.
        storage_location::Entity::update_many()
            .col_expr(
                storage_location::Column::LastDownloadedAt,
                Expr::value(Utc::now()),
            )
            .filter(storage_location::Column::Id.eq(location.id))
            .exec(&self.db)
            .await?;

        match self.open_location(&location).await {
            Ok(Some(stream)) => Ok(Some(Download {
                size: location.size_bytes as u64,
                stream: self.protect_download(stream, lease_id),
            })),
            result => {
                if let Err(err) = leases::release_reader_lease(&self.db, lease_id).await {
                    tracing::warn!(%lease_id, error = %err, "Failed to release Storage Reader Lease");
                }
                match result {
                    Err(Error::Storage(StorageError::NotFound(object))) => {
                        tracing::warn!(%cache_entry_id, "Stale cache entry: {object} is missing");
                        Ok(None)
                    }
                    Err(err) => Err(err),
                    Ok(_) => Ok(None),
                }
            }
        }
    }

    async fn open_location(
        &self,
        location: &storage_location::Model,
    ) -> Result<Option<ByteStream>> {
        if location.merged_at.is_some() {
            return Ok(Some(
                self.fs.read(&merged_name(&location.folder_name)).await?,
            ));
        }

        let parts = parts_folder(&location.folder_name);
        if self.fs.count_files(&parts).await? < location.part_count as usize {
            return Err(StorageError::NotFound(parts).into());
        }

        let (merger_sender, merger_receiver) = mpsc::channel::<std::io::Result<Bytes>>(2);
        let fs = self.fs.clone();
        let name = merged_name(&location.folder_name);
        let size = location.size_bytes as u64;
        let write = async move {
            fs.write(&name, ReceiverStream::new(merger_receiver), Some(size))
                .await
        };
        if !self.start_merge(location, write).await? {
            // Someone else is merging: read the Parts, which our Part Reader
            // Lease protects until we're done.
            return Ok(Some(stream_parts(
                self.fs.clone(),
                location.folder_name.clone(),
                location.part_count,
            )));
        }

        let (response_sender, response_receiver) = mpsc::channel::<std::io::Result<Bytes>>(2);
        let parts = stream_parts(
            self.fs.clone(),
            location.folder_name.clone(),
            location.part_count,
        );
        tokio::spawn(pump_parts(parts, response_sender, merger_sender));
        Ok(Some(Box::pin(ReceiverStream::new(response_receiver))))
    }

    /// Ties a Storage Reader Lease to a download stream: renewed while the
    /// stream is alive, released when it ends or is dropped, and the stream
    /// fails if the lease is lost.
    fn protect_download(&self, stream: ByteStream, lease_id: Uuid) -> ByteStream {
        let (lost_sender, mut lost) = oneshot::channel::<String>();
        let db = self.db.clone();
        let renewal = tokio::spawn(async move {
            let mut interval = tokio::time::interval_at(
                tokio::time::Instant::now() + LEASE_RENEWAL,
                LEASE_RENEWAL,
            );
            loop {
                interval.tick().await;
                match leases::renew_reader_lease(&db, lease_id).await {
                    Ok(true) => {}
                    Ok(false) => {
                        let _ = lost_sender.send("Storage Reader Lease was lost".into());
                        return;
                    }
                    Err(err) => {
                        let _ = lost_sender
                            .send(format!("Failed to renew Storage Reader Lease: {err}"));
                        return;
                    }
                }
            }
        });
        let guard = ReaderLeaseGuard {
            db: self.db.clone(),
            lease_id,
            renewal,
        };

        Box::pin(async_stream::stream! {
            let _guard = guard;
            let mut stream = stream;
            let mut watching = true;
            loop {
                let item = if watching {
                    tokio::select! {
                        biased;
                        lost_reason = &mut lost => match lost_reason {
                            Ok(reason) => {
                                yield Err(std::io::Error::other(reason));
                                break;
                            }
                            // The renewal task ended without reporting a loss.
                            Err(_) => {
                                watching = false;
                                continue;
                            }
                        },
                        item = stream.next() => item,
                    }
                } else {
                    stream.next().await
                };
                match item {
                    Some(item) => yield item,
                    None => break,
                }
            }
        })
    }

    /// Runs a Merge under a Merge Lease. `write` produces the merged object;
    /// a lease-fenced transaction then marks the Merge complete. Returns false
    /// when another worker holds the lease. Writing straight to the final
    /// object is safe because Parts are immutable (ADR-0004). The background
    /// task never fails: errors are logged and the merge state rolled back.
    async fn start_merge<W>(&self, location: &storage_location::Model, write: W) -> Result<bool>
    where
        W: Future<Output = Result<u64, StorageError>> + Send + 'static,
    {
        let location_id = location.id;
        let Some(token) = leases::acquire_merge_lease(&self.db, location_id).await? else {
            return Ok(false);
        };
        storage_location::Entity::update_many()
            .col_expr(
                storage_location::Column::MergeStartedAt,
                Expr::value(Utc::now()),
            )
            .filter(storage_location::Column::Id.eq(location_id))
            .exec(&self.db)
            .await?;

        let db = self.db.clone();
        self.merges.spawn(async move {
            let renewal = {
                let db = db.clone();
                tokio::spawn(async move {
                    let mut interval = tokio::time::interval_at(
                        tokio::time::Instant::now() + LEASE_RENEWAL,
                        LEASE_RENEWAL,
                    );
                    loop {
                        interval.tick().await;
                        // A lost lease is caught by the completion fence.
                        if let Err(err) = leases::renew_merge_lease(&db, location_id, token).await {
                            tracing::warn!(%location_id, error = %err, "Failed to renew Merge Lease");
                        }
                    }
                })
            };

            let result = async {
                write.await?;
                // The merged object is already written, so losing a deadlock
                // here must not throw the merge away; the fence is re-checked
                // on every attempt.
                let completed = retry_on_lock_conflict(|| complete_merge(&db, location_id, token)).await?;
                if !completed {
                    return Err(anyhow::anyhow!("Merge Lease was lost before completion"));
                }
                Ok(())
            }
            .await;

            if let Err(err) = result {
                tracing::error!(%location_id, error = format!("{err:#}"), "Merge failed");
                if let Err(err) = rollback_merge(&db, location_id, token).await {
                    tracing::error!(%location_id, error = %err, "Failed to roll back merge state");
                }
            }
            renewal.abort();
            if let Err(err) = leases::release_merge_lease(&db, location_id, token).await {
                tracing::warn!(%location_id, error = %err, "Failed to release Merge Lease");
            }
        });
        Ok(true)
    }

    /// Finds the best Cache Entry: per scope, the exact primary key, then the
    /// newest entry prefixed by it, then each restore key exactly and by prefix.
    pub async fn match_cache_entry(&self, query: &MatchQuery<'_>) -> Result<Option<CacheMatch>> {
        for scope in query.scopes {
            let candidates = std::iter::once((
                query.primary_key,
                MatchType::ExactPrimary,
                MatchType::PrefixedPrimary,
            ))
            .chain(query.restore_keys.iter().map(|key| {
                (
                    key.as_str(),
                    MatchType::ExactRestore,
                    MatchType::PrefixedRestore,
                )
            }));
            for (key, exact, prefixed) in candidates {
                for (match_type, condition) in [
                    (exact, cache_entry::Column::Key.eq(key)),
                    (
                        prefixed,
                        Expr::col((cache_entry::Entity, cache_entry::Column::Key))
                            .like(LikeExpr::new(format!("{}%", escape_like(key))).escape('\\')),
                    ),
                ] {
                    let entry = cache_entry::Entity::find()
                        .filter(cache_entry::Column::RepoId.eq(query.repo_id))
                        .filter(cache_entry::Column::Scope.eq(scope))
                        .filter(cache_entry::Column::Version.eq(query.version))
                        .filter(condition)
                        .order_by_desc(cache_entry::Column::UpdatedAt)
                        .one(&self.db)
                        .await?;
                    if let Some(entry) = entry {
                        return Ok(Some(CacheMatch { entry, match_type }));
                    }
                }
            }
        }
        Ok(None)
    }

    /// Matches a Cache Entry and returns its download URL. Returning a URL is
    /// a promise that the data exists — the client commits to downloading and
    /// can't walk a later failure back to a cache miss — so storage is
    /// validated first, and Dangling Cache Entries are purged and matching
    /// retried (ADR-0005).
    pub async fn cache_entry_download_url(
        &self,
        query: &MatchQuery<'_>,
    ) -> Result<Option<(String, cache_entry::Model)>> {
        for _ in 0..MAX_DANGLING_PURGE_ATTEMPTS {
            let Some(CacheMatch { entry, .. }) = self.match_cache_entry(query).await? else {
                return Ok(None);
            };
            let location = storage_location::Entity::find_by_id(entry.location_id)
                .one(&self.db)
                .await?;
            let valid = match &location {
                Some(location) => self.storage_has_data(location).await?,
                None => false,
            };
            if !valid {
                tracing::warn!(
                    cache_entry = %entry.id,
                    key = entry.key,
                    "Purging Dangling Cache Entry"
                );
                self.purge_dangling_cache_entry(&entry).await?;
                continue;
            }
            return Ok(Some((
                format!("{}/download/{}", self.config.api_base_url, entry.id),
                entry,
            )));
        }
        tracing::warn!("Exhausted Dangling Cache Entry purge attempts; returning cache miss");
        Ok(None)
    }

    /// A merged entry is confirmed by its merged object, an unmerged one by
    /// its first Part. Parts deleted without a completed merge means the data
    /// is gone. A single check suffices: external drift removes whole folders
    /// and the server never loses individual Parts (ADR-0005).
    async fn storage_has_data(&self, location: &storage_location::Model) -> Result<bool> {
        if location.merged_at.is_some() {
            return Ok(self.fs.exists(&merged_name(&location.folder_name)).await?);
        }
        if location.parts_deleted_at.is_some() || location.part_count == 0 {
            return Ok(false);
        }
        Ok(self.fs.exists(&part_name(&location.folder_name, 0)).await?)
    }

    async fn purge_dangling_cache_entry(&self, entry: &cache_entry::Model) -> Result<()> {
        let txn = self.db.begin().await?;
        cache_entry::Entity::delete_by_id(entry.id)
            .exec(&txn)
            .await?;
        // Reap the now-childless Storage Location too: an Orphaned Storage
        // sweep scans physical storage and can never see a row whose data is
        // already gone. Respects reader leases (ADR-0002).
        delete_storage_location_if_unread(&txn, entry.location_id).await?;
        txn.commit().await?;
        Ok(())
    }
}

/// Lease-fenced completion: marks the Merge done only if `token` still holds
/// an unexpired Merge Lease.
async fn complete_merge(
    db: &DatabaseConnection,
    location_id: Uuid,
    token: Uuid,
) -> Result<bool, DbErr> {
    let txn = db.begin().await?;
    let lease = merge_lease::Entity::find_by_id(location_id)
        .lock_exclusive()
        .one(&txn)
        .await?;
    if !lease.is_some_and(|lease| lease.token == token && lease.expires_at > Utc::now()) {
        return Ok(false);
    }
    storage_location::Entity::update_many()
        .col_expr(storage_location::Column::MergedAt, Expr::value(Utc::now()))
        .filter(storage_location::Column::Id.eq(location_id))
        .exec(&txn)
        .await?;
    txn.commit().await?;
    Ok(true)
}

async fn rollback_merge(
    db: &DatabaseConnection,
    location_id: Uuid,
    token: Uuid,
) -> Result<(), DbErr> {
    let lease_held = Query::select()
        .expr(Expr::val(1))
        .from(merge_lease::Entity)
        .and_where(merge_lease::Column::StorageLocationId.eq(location_id))
        .and_where(merge_lease::Column::Token.eq(token))
        .to_owned();
    storage_location::Entity::update_many()
        .col_expr(
            storage_location::Column::MergedAt,
            Expr::val(None::<chrono::DateTime<Utc>>),
        )
        .col_expr(
            storage_location::Column::MergeStartedAt,
            Expr::val(None::<chrono::DateTime<Utc>>),
        )
        .filter(storage_location::Column::Id.eq(location_id))
        .filter(Expr::exists(lease_held))
        .exec(db)
        .await?;
    Ok(())
}

/// Feeds Part bytes to both the client and the merger, each with
/// backpressure. A client that goes away doesn't stop the merge; a merger
/// failure doesn't stop the client. A read error is passed to both, so the
/// merger never mistakes a truncated stream for a complete one (it also checks
/// the byte count).
async fn pump_parts(
    mut parts: ByteStream,
    response: mpsc::Sender<std::io::Result<Bytes>>,
    merger: mpsc::Sender<std::io::Result<Bytes>>,
) {
    let mut response = Some(response);
    let mut merger = Some(merger);
    while let Some(chunk) = parts.next().await {
        let chunk = match chunk {
            Ok(chunk) => chunk,
            Err(err) => {
                let message = err.to_string();
                if let Some(response) = &response {
                    let _ = response
                        .send(Err(std::io::Error::new(err.kind(), message.clone())))
                        .await;
                }
                if let Some(merger) = &merger {
                    let _ = merger
                        .send(Err(std::io::Error::new(err.kind(), message)))
                        .await;
                }
                return;
            }
        };
        let to_response = async {
            match &response {
                Some(sender) => sender.send(Ok(chunk.clone())).await.is_ok(),
                None => false,
            }
        };
        let to_merger = async {
            match &merger {
                Some(sender) => sender.send(Ok(chunk.clone())).await.is_ok(),
                None => false,
            }
        };
        let (response_open, merger_open) = tokio::join!(to_response, to_merger);
        if !response_open {
            response = None;
        }
        if !merger_open {
            merger = None;
        }
        if response.is_none() && merger.is_none() {
            return;
        }
    }
}

/// Stops renewing and releases a Storage Reader Lease when its download stream
/// is dropped, whether it finished, failed or the client went away.
struct ReaderLeaseGuard {
    db: DatabaseConnection,
    lease_id: Uuid,
    renewal: tokio::task::JoinHandle<()>,
}

impl Drop for ReaderLeaseGuard {
    fn drop(&mut self) {
        self.renewal.abort();
        let db = self.db.clone();
        let lease_id = self.lease_id;
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            // An unreleased lease only delays cleanup until it expires.
            runtime.spawn(async move {
                if let Err(err) = leases::release_reader_lease(&db, lease_id).await {
                    tracing::warn!(%lease_id, error = %err, "Failed to release Storage Reader Lease");
                }
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn escapes_like_wildcards() {
        assert_eq!(escape_like(r"a%b_c\d"), r"a\%b\_c\\d");
    }
}
