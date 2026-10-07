//! Scheduled cleanup tasks, ported one-to-one from the original Nitro tasks.
//! Database state is committed before physical deletion (ADR-0001), and every
//! deletion respects Storage Reader Leases (ADR-0002).

use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use sea_orm::sea_query::{Expr, Query};
use sea_orm::{
    ColumnTrait, Condition, DatabaseConnection, EntityTrait, ExprTrait, JoinType, QueryFilter,
    QuerySelect, RelationTrait, TransactionTrait,
};
use serde::Serialize;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::config::Config;
use crate::entity::storage_reader_lease::ReaderScope;
use crate::entity::{cache_entry, merge_lease, storage_location, storage_reader_lease, upload};
use crate::storage::Storage;
use crate::storage::lifecycle::{
    claim_parts_deletion_if_unread, delete_storage_location_if_unread, no_active_reader_lease,
    reconcile_orphaned_storage,
};

const PAGE_SIZE: u64 = 10;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Task {
    Uploads,
    CacheEntries,
    OrphanedStorage,
    Parts,
    StorageLocations,
    Merges,
}

impl Task {
    pub const ALL: [Task; 6] = [
        Task::Uploads,
        Task::CacheEntries,
        Task::OrphanedStorage,
        Task::Parts,
        Task::StorageLocations,
        Task::Merges,
    ];

    pub fn name(self) -> &'static str {
        match self {
            Task::Uploads => "cleanup:uploads",
            Task::CacheEntries => "cleanup:cache-entries",
            Task::OrphanedStorage => "cleanup:orphaned-storage",
            Task::Parts => "cleanup:parts",
            Task::StorageLocations => "cleanup:storage-locations",
            Task::Merges => "cleanup:merges",
        }
    }

    /// Runs whenever the UTC wall clock is a multiple of this period:
    /// uploads every 5 minutes, parts and unreferenced storage every 10,
    /// merges hourly, retention and orphan sweeps daily at midnight.
    fn period(self) -> Duration {
        Duration::from_secs(match self {
            Task::Uploads => 5 * 60,
            Task::Parts | Task::StorageLocations => 10 * 60,
            Task::Merges => 60 * 60,
            Task::CacheEntries | Task::OrphanedStorage => 24 * 60 * 60,
        })
    }

    fn index(self) -> usize {
        Task::ALL
            .iter()
            .position(|task| *task == self)
            .expect("listed")
    }
}

/// Counters every task reports when it finishes.
#[derive(Clone, Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Summary {
    pub skipped: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deleted_uploads: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deleted_locations: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deleted_parts: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deleted_objects: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deleted_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reset_merges: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub orphaned_storage: Option<crate::storage::lifecycle::OrphanedStorageSummary>,
    pub failures: u64,
    pub duration_ms: u64,
}

impl Summary {
    fn add(counter: &mut Option<u64>, amount: u64) {
        *counter = Some(counter.unwrap_or(0) + amount);
    }
}

pub struct Cleanup {
    db: DatabaseConnection,
    storage: Arc<Storage>,
    config: Arc<Config>,
    /// One run of each task at a time per process (Nitro's `runTask` dedupes
    /// the same way); concurrent replicas are kept safe by row locks.
    running: [Mutex<()>; 6],
}

impl Cleanup {
    pub fn new(storage: Arc<Storage>, config: Arc<Config>) -> Self {
        Self {
            db: storage.db().clone(),
            storage,
            config,
            running: Default::default(),
        }
    }

    /// Runs a task and logs its summary.
    pub async fn run(&self, task: Task) -> anyhow::Result<Summary> {
        let _running = self.running[task.index()].lock().await;
        let started = std::time::Instant::now();
        let mut summary = Summary {
            skipped: self.config.disable_cleanup_jobs
                || (task == Task::CacheEntries && self.config.cache_cleanup_older_than_days == 0.0),
            ..Default::default()
        };
        let result = if summary.skipped {
            Ok(())
        } else {
            match task {
                Task::Uploads => self.uploads(&mut summary).await,
                Task::CacheEntries => self.cache_entries(&mut summary).await,
                Task::OrphanedStorage => self.orphaned_storage(&mut summary).await,
                Task::Parts => self.parts(&mut summary).await,
                Task::StorageLocations => self.storage_locations(&mut summary).await,
                Task::Merges => self.merges(&mut summary).await,
            }
        };
        summary.duration_ms = started.elapsed().as_millis() as u64;
        if result.is_err() && summary.failures == 0 {
            summary.failures = 1;
        }
        let fields = serde_json::to_string(&summary).unwrap_or_default();
        match result {
            Ok(()) => {
                tracing::info!(
                    task = task.name(),
                    summary = fields,
                    "Cleanup run completed"
                );
                Ok(summary)
            }
            Err(err) => {
                tracing::error!(
                    task = task.name(),
                    summary = fields,
                    error = format!("{err:#}"),
                    "Cleanup run failed"
                );
                Err(err)
            }
        }
    }

    /// Runs the task in the background, e.g. after a management API delete.
    pub fn trigger(self: &Arc<Self>, task: Task) {
        let this = self.clone();
        tokio::spawn(async move {
            let _ = this.run(task).await;
        });
    }

    /// Deletes storage folders whose database rows are already gone, counting
    /// failures instead of stopping.
    async fn delete_folders(
        &self,
        summary: &mut Summary,
        errors: &mut Vec<anyhow::Error>,
        folder: &str,
    ) {
        match self.storage.fs().delete_folder(folder).await {
            Ok(deleted) => {
                Summary::add(&mut summary.deleted_objects, deleted.objects);
                Summary::add(&mut summary.deleted_bytes, deleted.bytes);
            }
            Err(err) => {
                summary.failures += 1;
                errors.push(err.into());
            }
        }
    }

    /// Delete uploads without activity for over 1 minute.
    async fn uploads(&self, summary: &mut Summary) -> anyhow::Result<()> {
        let mut errors = Vec::new();
        Summary::add(&mut summary.deleted_uploads, 0);
        loop {
            let cutoff = Utc::now() - chrono::Duration::minutes(1);
            let uploads = upload::Entity::find()
                .select_only()
                .columns([upload::Column::Id, upload::Column::FolderName])
                .filter(upload::Column::CreatedAt.lt(cutoff))
                .filter(
                    Condition::any()
                        .add(upload::Column::LastPartUploadedAt.is_null())
                        .add(upload::Column::LastPartUploadedAt.lt(cutoff)),
                )
                .limit(PAGE_SIZE)
                .into_tuple::<(i64, String)>()
                .all(&self.db)
                .await?;
            for (id, folder_name) in &uploads {
                upload::Entity::delete_by_id(*id).exec(&self.db).await?;
                Summary::add(&mut summary.deleted_uploads, 1);
                self.delete_folders(summary, &mut errors, folder_name).await;
            }
            if (uploads.len() as u64) < PAGE_SIZE {
                break;
            }
        }
        failures(errors, "Failed to delete abandoned upload storage")
    }

    /// Delete cache entries neither saved nor accessed within the retention
    /// period.
    async fn cache_entries(&self, summary: &mut Summary) -> anyhow::Result<()> {
        let cutoff = Utc::now()
            - chrono::Duration::milliseconds(
                (self.config.cache_cleanup_older_than_days * 24.0 * 60.0 * 60.0 * 1000.0) as i64,
            );
        let mut errors = Vec::new();
        Summary::add(&mut summary.deleted_locations, 0);
        let mut skipped = std::collections::HashSet::new();
        loop {
            let locations = storage_location::Entity::find()
                .select_only()
                .columns([
                    storage_location::Column::Id,
                    storage_location::Column::FolderName,
                ])
                .join(
                    JoinType::InnerJoin,
                    storage_location::Relation::CacheEntry.def(),
                )
                .filter(cache_entry::Column::UpdatedAt.lt(cutoff))
                .filter(
                    Condition::any()
                        .add(storage_location::Column::LastDownloadedAt.is_null())
                        .add(storage_location::Column::LastDownloadedAt.lt(cutoff)),
                )
                .filter(no_active_reader_lease(None))
                .filter(storage_location::Column::Id.is_not_in(skipped.iter().copied()))
                .limit(PAGE_SIZE)
                .into_tuple::<(Uuid, String)>()
                .all(&self.db)
                .await?;
            self.delete_locations(summary, &mut errors, &mut skipped, &locations)
                .await?;
            if (locations.len() as u64) < PAGE_SIZE {
                break;
            }
        }
        failures(errors, "Failed to delete retained cache storage")
    }

    /// Delete storage locations no cache entry references.
    async fn storage_locations(&self, summary: &mut Summary) -> anyhow::Result<()> {
        let mut errors = Vec::new();
        Summary::add(&mut summary.deleted_locations, 0);
        let mut skipped = std::collections::HashSet::new();
        loop {
            let referenced = Query::select()
                .expr(Expr::val(1))
                .from(cache_entry::Entity)
                .and_where(
                    Expr::col((cache_entry::Entity, cache_entry::Column::LocationId))
                        .equals((storage_location::Entity, storage_location::Column::Id)),
                )
                .to_owned();
            let locations = storage_location::Entity::find()
                .select_only()
                .columns([
                    storage_location::Column::Id,
                    storage_location::Column::FolderName,
                ])
                .filter(Expr::exists(referenced).not())
                .filter(no_active_reader_lease(None))
                .filter(storage_location::Column::Id.is_not_in(skipped.iter().copied()))
                .limit(PAGE_SIZE)
                .into_tuple::<(Uuid, String)>()
                .all(&self.db)
                .await?;
            self.delete_locations(summary, &mut errors, &mut skipped, &locations)
                .await?;
            if (locations.len() as u64) < PAGE_SIZE {
                break;
            }
        }
        failures(errors, "Failed to delete unreferenced storage")
    }

    async fn delete_locations(
        &self,
        summary: &mut Summary,
        errors: &mut Vec<anyhow::Error>,
        skipped: &mut std::collections::HashSet<Uuid>,
        locations: &[(Uuid, String)],
    ) -> anyhow::Result<()> {
        for (id, folder_name) in locations {
            let txn = self.db.begin().await?;
            let deleted = delete_storage_location_if_unread(&txn, *id).await?;
            txn.commit().await?;
            if !deleted {
                // A reader took a lease since the page was read; don't spin on it.
                skipped.insert(*id);
                continue;
            }
            Summary::add(&mut summary.deleted_locations, 1);
            self.delete_folders(summary, errors, folder_name).await;
        }
        Ok(())
    }

    /// Delete the Parts of merged cache entries once no Part Reader Lease
    /// protects them.
    async fn parts(&self, summary: &mut Summary) -> anyhow::Result<()> {
        let mut errors = Vec::new();
        Summary::add(&mut summary.deleted_parts, 0);
        let mut skipped = std::collections::HashSet::new();
        loop {
            let locations = storage_location::Entity::find()
                .select_only()
                .columns([
                    storage_location::Column::Id,
                    storage_location::Column::FolderName,
                ])
                .filter(storage_location::Column::MergedAt.is_not_null())
                .filter(storage_location::Column::PartsDeletedAt.is_null())
                .filter(no_active_reader_lease(Some(ReaderScope::Parts)))
                .filter(storage_location::Column::Id.is_not_in(skipped.iter().copied()))
                .limit(PAGE_SIZE)
                .into_tuple::<(Uuid, String)>()
                .all(&self.db)
                .await?;
            for (id, folder_name) in &locations {
                let txn = self.db.begin().await?;
                let claimed = claim_parts_deletion_if_unread(&txn, *id).await?;
                txn.commit().await?;
                if !claimed {
                    skipped.insert(*id);
                    continue;
                }
                match self
                    .storage
                    .fs()
                    .delete_folder(&format!("{folder_name}/parts"))
                    .await
                {
                    Ok(deleted) => {
                        Summary::add(&mut summary.deleted_parts, deleted.objects);
                        Summary::add(&mut summary.deleted_bytes, deleted.bytes);
                    }
                    Err(err) => {
                        // Give the claim back so the next run retries.
                        storage_location::Entity::update_many()
                            .col_expr(
                                storage_location::Column::PartsDeletedAt,
                                Expr::val(None::<chrono::DateTime<Utc>>),
                            )
                            .filter(storage_location::Column::Id.eq(*id))
                            .exec(&self.db)
                            .await?;
                        skipped.insert(*id);
                        summary.failures += 1;
                        errors.push(err.into());
                    }
                }
            }
            if (locations.len() as u64) < PAGE_SIZE {
                break;
            }
        }
        failures(errors, "Failed to delete merged cache parts")
    }

    /// Reset stalled merges and purge expired leases.
    async fn merges(&self, summary: &mut Summary) -> anyhow::Result<()> {
        let now = Utc::now();
        let live_lease = Query::select()
            .expr(Expr::val(1))
            .from(merge_lease::Entity)
            .and_where(
                Expr::col((merge_lease::Entity, merge_lease::Column::StorageLocationId))
                    .equals((storage_location::Entity, storage_location::Column::Id)),
            )
            .and_where(Expr::col((merge_lease::Entity, merge_lease::Column::ExpiresAt)).gt(now))
            .to_owned();
        let reset = storage_location::Entity::update_many()
            .col_expr(
                storage_location::Column::MergeStartedAt,
                Expr::val(None::<chrono::DateTime<Utc>>),
            )
            .filter(
                storage_location::Column::MergeStartedAt.lt(now - chrono::Duration::minutes(15)),
            )
            .filter(storage_location::Column::MergedAt.is_null())
            .filter(Expr::exists(live_lease).not())
            .exec(&self.db)
            .await?;
        summary.reset_merges = Some(reset.rows_affected);

        merge_lease::Entity::delete_many()
            .filter(merge_lease::Column::ExpiresAt.lte(now))
            .exec(&self.db)
            .await?;
        // Expired Storage Reader Leases are otherwise only removed by stream
        // teardown or the location's cascade.
        storage_reader_lease::Entity::delete_many()
            .filter(storage_reader_lease::Column::ExpiresAt.lte(now))
            .exec(&self.db)
            .await?;
        Ok(())
    }

    /// Delete grace-expired storage folders no database record authorizes.
    async fn orphaned_storage(&self, summary: &mut Summary) -> anyhow::Result<()> {
        let result = reconcile_orphaned_storage(
            &self.db,
            self.storage.fs(),
            self.config.orphaned_storage_grace_period_hours,
            Utc::now(),
        )
        .await;
        match result {
            Ok(orphans) => {
                summary.orphaned_storage = Some(orphans);
                Ok(())
            }
            Err((orphans, err)) => {
                summary.failures = orphans.failures;
                summary.orphaned_storage = Some(orphans);
                Err(err)
            }
        }
    }
}

fn failures(errors: Vec<anyhow::Error>, message: &str) -> anyhow::Result<()> {
    let count = errors.len();
    match errors.into_iter().next() {
        None => Ok(()),
        Some(first) => Err(first.context(format!("{message} ({count} failures)"))),
    }
}

/// Runs each task on its schedule until `shutdown` is cancelled.
pub fn spawn_scheduler(cleanup: Arc<Cleanup>, shutdown: CancellationToken) {
    for task in Task::ALL {
        let cleanup = cleanup.clone();
        let shutdown = shutdown.clone();
        tokio::spawn(async move {
            loop {
                let period = task.period().as_millis() as i64;
                let now = Utc::now().timestamp_millis();
                let wait = Duration::from_millis((period - now.rem_euclid(period)) as u64);
                tokio::select! {
                    _ = shutdown.cancelled() => return,
                    _ = tokio::time::sleep(wait) => {}
                }
                let _ = cleanup.run(task).await;
            }
        });
    }
}
