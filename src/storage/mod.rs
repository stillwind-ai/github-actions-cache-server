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
use sea_orm::sea_query::{CaseStatement, Expr, Func, LikeExpr, OnConflict, Query};
use sea_orm::{
    ActiveValue::Set, ColumnTrait, Condition, DatabaseConnection, DbErr, EntityTrait, ExprTrait,
    JoinType, Order, QueryFilter, QueryOrder, QuerySelect, RelationTrait, TransactionTrait,
};
use serde::Serialize;
use tokio::sync::oneshot;
use tokio_util::task::TaskTracker;
use uuid::Uuid;

use self::fs::{FsStorage, StorageError};
use self::io::ByteStream;
use self::leases::LEASE_RENEWAL;
use self::lifecycle::{delete_storage_location_if_unread, no_active_reader_lease};
use crate::config::Config;
use crate::db::retry_on_lock_conflict;
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

/// `percent`% of `bytes`, rounded down.
#[expect(
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss,
    reason = "a byte budget needs no more than f64 precision; float-to-int `as` saturates"
)]
fn percent_of(bytes: u64, percent: f64) -> u64 {
    (bytes as f64 * percent / 100.0).floor() as u64
}

/// Streams the Parts of a Storage Location in order, opening each lazily.
fn stream_parts(fs: FsStorage, folder_name: String, part_count: i32) -> ByteStream {
    Box::pin(async_stream::stream! {
        for index in 0..u32::try_from(part_count).unwrap_or(0) {
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
    #[must_use]
    pub fn new(db: DatabaseConnection, fs: FsStorage, config: Arc<Config>) -> Self {
        Self {
            db,
            fs,
            config,
            merges: TaskTracker::new(),
        }
    }

    #[must_use]
    pub fn fs(&self) -> &FsStorage {
        &self.fs
    }

    #[must_use]
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
    ///
    /// # Errors
    ///
    /// If the database query fails.
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
    ///
    /// # Errors
    ///
    /// If the database update or writing the Part fails.
    pub async fn upload_part<S>(&self, upload_id: i64, index: u32, body: S) -> Result<bool>
    where
        S: Stream<Item = std::io::Result<Bytes>> + Send + 'static,
    {
        // Counting the started Part also finds the Upload, in one round trip.
        let Some(upload) = upload::Entity::update_many()
            .col_expr(
                upload::Column::StartedPartUploadCount,
                Expr::col(upload::Column::StartedPartUploadCount).add(1),
            )
            .filter(upload::Column::Id.eq(upload_id))
            .exec_with_returning(&self.db)
            .await?
            .pop()
        else {
            return Ok(false);
        };
        let folder_name = upload.folder_name;

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
    /// Like a completion, an abandonment claims the Upload first: a
    /// concurrent finalization may already have turned it into a Storage
    /// Location (and renamed its only Part, so the Parts look missing), and
    /// then its folder holds live data. `Ok(None)` when it was claimed.
    async fn abandon_upload(&self, upload: &upload::Model, reason: String) -> Result<Option<i64>> {
        let claimed = upload::Entity::delete_by_id(upload.id)
            .exec(&self.db)
            .await?;
        if claimed.rows_affected != 1 {
            return Ok(None);
        }
        if let Err(err) = self.fs.delete_folder(&upload.folder_name).await {
            tracing::warn!(upload = upload.id, error = %err, "Failed to delete abandoned upload");
        }
        Err(Error::UploadRejected(reason))
    }

    /// Turns a finished Upload into a Cache Entry, replacing an existing entry
    /// for the same key. Returns the Upload's id, or `None` if there is no
    /// such Upload.
    ///
    /// # Errors
    ///
    /// [`Error::UploadRejected`] when the Upload's Parts are missing or
    /// inconsistent, after abandoning it; otherwise database and storage errors.
    #[expect(
        clippy::too_many_lines,
        reason = "one transaction-shaped sequence of checks and writes"
    )]
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
            return self
                .abandon_upload(&upload, "No parts have been uploaded".into())
                .await;
        }
        if upload.started_part_upload_count != upload.finished_part_upload_count {
            let reason = format!(
                "Not all parts have been uploaded (only {} of {} parts uploaded)",
                upload.finished_part_upload_count, upload.started_part_upload_count
            );
            return self.abandon_upload(&upload, reason).await;
        }

        let parts = self
            .fs
            .list_folder(&parts_folder(&upload.folder_name))
            .await?;
        if usize::try_from(upload.finished_part_upload_count) != Ok(parts.len()) {
            let reason = format!(
                "Uploaded part count does not match actual part count in storage (expected {} but found {})",
                upload.finished_part_upload_count,
                parts.len()
            );
            return self.abandon_upload(&upload, reason).await;
        }
        // Downloads read parts 0..n-1, so anything else would be a broken entry.
        let indices: BTreeSet<usize> = parts
            .iter()
            .filter_map(|part| part.name.parse().ok())
            .collect();
        if !indices.iter().copied().eq(0..parts.len()) {
            return self
                .abandon_upload(
                    &upload,
                    "Uploaded parts are not numbered contiguously from 0".into(),
                )
                .await;
        }

        let now = Utc::now();
        // A single Part already is the merged object: it is renamed into
        // place below instead of being copied by a Merge.
        let merged = (parts.len() == 1).then_some(now);
        let location = storage_location::ActiveModel {
            id: Set(Uuid::new_v4()),
            folder_name: Set(upload.folder_name.clone()),
            part_count: Set(upload.finished_part_upload_count),
            size_bytes: Set(parts.iter().map(|part| part.bytes.cast_signed()).sum()),
            created_at: Set(now),
            merge_started_at: Set(merged),
            merged_at: Set(merged),
            parts_deleted_at: Set(merged),
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
        // Before the commit, so the location never names a missing object
        // (which a lookup would purge as dangling, ADR-0005). The Upload is
        // claimed, so no one else reads or renames its Part.
        if merged.is_some() {
            self.fs
                .rename(
                    &part_name(&upload.folder_name, 0),
                    &merged_name(&upload.folder_name),
                )
                .await?;
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

        if self.config.eager_merge
            && merged.is_none()
            && let Err(err) = self.start_merge(&location).await
        {
            tracing::warn!(error = %err, "Eager Merge failed to start after upload completion");
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
    ///
    /// # Errors
    ///
    /// If the database query fails.
    pub async fn total_stored_bytes(&self) -> Result<u64> {
        self.stored_bytes().await
    }

    /// Capacity-based Eviction (ADR-0008): once usage exceeds the Storage
    /// Budget, deletes Cache Entries in Cache Recency order until usage is at
    /// most 90% of it. Reader leases still protect locations being read.
    ///
    /// # Errors
    ///
    /// If storage usage can't be read, or the database or a deletion fails.
    pub async fn enforce_storage_budget(&self) -> Result<EvictionSummary> {
        let mut summary = EvictionSummary::default();
        let filesystem_usage = match self.config.cache_max_size_bytes {
            Some(_) => None,
            None => Some(self.fs.filesystem_usage().await?),
        };
        let budget = match (self.config.cache_max_size_bytes, filesystem_usage) {
            (Some(max), _) => max,
            (None, Some(usage)) => percent_of(
                usage.capacity_bytes,
                self.config.cache_filesystem_max_usage_percent,
            ),
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
                None => usage.saturating_sub(size_bytes.cast_unsigned()),
            };
        }
        Ok(summary)
    }

    /// Starts a proxied download, taking a Storage Reader Lease for its
    /// lifetime. The first download of an unmerged entry also starts the
    /// Merge, which runs on its own while the download streams the Parts.
    /// `None` when the entry doesn't exist or its data is gone.
    ///
    /// # Errors
    ///
    /// If the database or storage fails. Missing data is `Ok(None)`, not an error.
    pub async fn download(&self, cache_entry_id: Uuid) -> Result<Option<Download>> {
        let Some((location, lease_id)) =
            leases::lease_for_download(&self.db, cache_entry_id).await?
        else {
            return Ok(None);
        };

        match self.open_location(&location).await {
            Ok(Some(stream)) => Ok(Some(Download {
                size: location.size_bytes.cast_unsigned(),
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
        if self.fs.count_files(&parts).await? < usize::try_from(location.part_count).unwrap_or(0) {
            return Err(StorageError::NotFound(parts).into());
        }
        // Whether this download starts the Merge or another worker holds the
        // Merge Lease, our Part Reader Lease protects the Parts we stream.
        self.start_merge(location).await?;
        Ok(Some(stream_parts(
            self.fs.clone(),
            location.folder_name.clone(),
            location.part_count,
        )))
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
                        lost_reason = &mut lost => if let Ok(reason) = lost_reason {
                            yield Err(std::io::Error::other(reason));
                            break;
                        } else {
                            // The renewal task ended without reporting a loss.
                            watching = false;
                            continue;
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

    /// Runs a Merge under a Merge Lease in the background: the Parts are
    /// concatenated into the merged object inside the kernel, then a
    /// lease-fenced transaction marks the Merge complete. Returns false when
    /// another worker holds the lease. The background task never fails:
    /// errors are logged and the merge state rolled back.
    async fn start_merge(&self, location: &storage_location::Model) -> Result<bool> {
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

        let fs = self.fs.clone();
        let parts: Vec<String> = (0..location.part_count.cast_unsigned())
            .map(|index| part_name(&location.folder_name, index))
            .collect();
        let merged = merged_name(&location.folder_name);
        let size = location.size_bytes.cast_unsigned();
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
                // Parts are immutable (ADR-0004) and the merged object only
                // becomes visible once its length is verified.
                fs.concat(&parts, &merged, size).await?;
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
    /// newest entry prefixed by it, then each restore key exactly and by
    /// prefix.
    ///
    /// # Errors
    ///
    /// If the database fails.
    pub async fn match_cache_entry(&self, query: &MatchQuery<'_>) -> Result<Option<CacheMatch>> {
        Ok(self.find_match(query).await?.map(|(found, _)| found))
    }

    /// [`Self::match_cache_entry`] in one query, with the entry's Storage
    /// Location. Every candidate is ranked by the first (scope, key, exact or
    /// prefix) level it satisfies, so the lowest rank, newest first, is the
    /// entry that checking the levels one by one would find, without a round
    /// trip per level.
    async fn find_match(
        &self,
        query: &MatchQuery<'_>,
    ) -> Result<Option<(CacheMatch, Option<storage_location::Model>)>> {
        let keys: Vec<&str> = std::iter::once(query.primary_key)
            .chain(query.restore_keys.iter().map(String::as_str))
            .collect();
        let scope = || Expr::col((cache_entry::Entity, cache_entry::Column::Scope));
        let key = || Expr::col((cache_entry::Entity, cache_entry::Column::Key));
        let prefixed = |prefix: &str| {
            key().like(LikeExpr::new(format!("{}%", escape_like(prefix))).escape('\\'))
        };
        let mut rank = CaseStatement::new();
        let mut level = 0;
        for scope_name in query.scopes {
            for candidate in &keys {
                for condition in [key().eq(*candidate), prefixed(candidate)] {
                    rank = rank.case(
                        Condition::all()
                            .add(scope().eq(scope_name.as_str()))
                            .add(condition),
                        Expr::val(level),
                    );
                    level += 1;
                }
            }
        }
        let Some((entry, location)) = cache_entry::Entity::find()
            .find_also_related(storage_location::Entity)
            .filter(cache_entry::Column::RepoId.eq(query.repo_id))
            .filter(cache_entry::Column::Version.eq(query.version))
            .filter(cache_entry::Column::Scope.is_in(query.scopes))
            .filter(keys.iter().fold(Condition::any(), |any, candidate| {
                any.add(prefixed(candidate))
            }))
            .order_by(
                Into::<Expr>::into(rank.finally(Expr::val(level))),
                Order::Asc,
            )
            .order_by_desc(cache_entry::Column::UpdatedAt)
            .one(&self.db)
            .await?
        else {
            return Ok(None);
        };
        let match_type = keys
            .iter()
            .enumerate()
            .find_map(|(index, candidate)| {
                let (exact, prefixed) = if index == 0 {
                    (MatchType::ExactPrimary, MatchType::PrefixedPrimary)
                } else {
                    (MatchType::ExactRestore, MatchType::PrefixedRestore)
                };
                if entry.key == *candidate {
                    Some(exact)
                } else {
                    entry.key.starts_with(candidate).then_some(prefixed)
                }
            })
            .expect("the query only returns entries matching a key");
        Ok(Some((CacheMatch { entry, match_type }, location)))
    }

    /// Matches a Cache Entry and returns its download URL. Returning a URL is
    /// a promise that the data exists — the client commits to downloading and
    /// can't walk a later failure back to a cache miss — so storage is
    /// validated first, and Dangling Cache Entries are purged and matching
    /// retried (ADR-0005).
    ///
    /// # Errors
    ///
    /// If the database or a storage existence check fails.
    pub async fn cache_entry_download_url(
        &self,
        query: &MatchQuery<'_>,
    ) -> Result<Option<(String, cache_entry::Model)>> {
        for _ in 0..MAX_DANGLING_PURGE_ATTEMPTS {
            let Some((CacheMatch { entry, .. }, location)) = self.find_match(query).await? else {
                return Ok(None);
            };
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
