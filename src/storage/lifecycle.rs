//! Lease-respecting deletion and Orphaned Storage reconciliation (ADR-0001,
//! ADR-0002).

use std::collections::HashSet;

use chrono::{DateTime, Utc};
use futures::StreamExt;
use sea_orm::sea_query::{Expr, Query, SimpleExpr};
use sea_orm::{
    ColumnTrait, ConnectionTrait, DatabaseConnection, DbErr, EntityTrait, ExprTrait, QueryFilter,
    QuerySelect,
};
use serde::Serialize;
use uuid::Uuid;

use super::backend::{Backend, StorageError};
use crate::entity::storage_reader_lease::ReaderScope;
use crate::entity::{storage_location, storage_reader_lease, upload};

/// Row-locks a Storage Location for the rest of the transaction. False when
/// it no longer exists.
pub async fn lock_storage_location(db: &impl ConnectionTrait, id: Uuid) -> Result<bool, DbErr> {
    let locked = storage_location::Entity::find_by_id(id)
        .select_only()
        .column(storage_location::Column::Id)
        .lock_exclusive()
        .into_tuple::<Uuid>()
        .one(db)
        .await?;
    Ok(locked.is_some())
}

async fn has_active_reader_lease(
    db: &impl ConnectionTrait,
    storage_location_id: Uuid,
    scope: Option<ReaderScope>,
) -> Result<bool, DbErr> {
    let mut query = storage_reader_lease::Entity::find()
        .filter(storage_reader_lease::Column::StorageLocationId.eq(storage_location_id))
        .filter(storage_reader_lease::Column::ExpiresAt.gt(Utc::now()));
    if let Some(scope) = scope {
        query = query.filter(storage_reader_lease::Column::Scope.eq(scope));
    }
    Ok(query.one(db).await?.is_some())
}

/// Predicate for queries over `storage_locations`: true when no unexpired
/// Storage Reader Lease (optionally of one scope) protects the row.
pub fn no_active_reader_lease(scope: Option<ReaderScope>) -> SimpleExpr {
    let mut leases = Query::select();
    leases
        .expr(Expr::val(1))
        .from(storage_reader_lease::Entity)
        .and_where(
            Expr::col((
                storage_reader_lease::Entity,
                storage_reader_lease::Column::StorageLocationId,
            ))
            .equals((storage_location::Entity, storage_location::Column::Id)),
        )
        .and_where(
            Expr::col((
                storage_reader_lease::Entity,
                storage_reader_lease::Column::ExpiresAt,
            ))
            .gt(Utc::now()),
        );
    if let Some(scope) = scope {
        leases.and_where(
            Expr::col((
                storage_reader_lease::Entity,
                storage_reader_lease::Column::Scope,
            ))
            .eq(scope),
        );
    }
    Expr::exists(leases).not()
}

/// Deletes a Storage Location (cascading to its Cache Entries and leases)
/// unless a reader holds it. Must run inside a transaction; the physical
/// folder is deleted by the caller after commit.
pub async fn delete_storage_location_if_unread(
    tx: &impl ConnectionTrait,
    storage_location_id: Uuid,
) -> Result<bool, DbErr> {
    if !lock_storage_location(tx, storage_location_id).await?
        || has_active_reader_lease(tx, storage_location_id, None).await?
    {
        return Ok(false);
    }
    storage_location::Entity::delete_by_id(storage_location_id)
        .exec(tx)
        .await?;
    Ok(true)
}

/// Marks a merged Storage Location's Parts as deleted unless a Part Reader
/// Lease protects them. Must run inside a transaction.
pub async fn claim_parts_deletion_if_unread(
    tx: &impl ConnectionTrait,
    storage_location_id: Uuid,
) -> Result<bool, DbErr> {
    if !lock_storage_location(tx, storage_location_id).await?
        || has_active_reader_lease(tx, storage_location_id, Some(ReaderScope::Parts)).await?
    {
        return Ok(false);
    }
    let result = storage_location::Entity::update_many()
        .col_expr(
            storage_location::Column::PartsDeletedAt,
            Expr::value(Utc::now()),
        )
        .filter(storage_location::Column::Id.eq(storage_location_id))
        .filter(storage_location::Column::MergedAt.is_not_null())
        .filter(storage_location::Column::PartsDeletedAt.is_null())
        .exec(tx)
        .await?;
    Ok(result.rows_affected == 1)
}

#[derive(Clone, Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OrphanedStorageSummary {
    pub inspected_folders: u64,
    pub authorized_folders: u64,
    pub grace_period_folders: u64,
    pub deleted_folders: u64,
    pub deleted_objects: u64,
    pub deleted_bytes: u64,
    pub failures: u64,
}

/// Deletes top-level storage that neither an Upload nor a Storage Location
/// authorizes, once its newest object is older than the grace period.
pub async fn reconcile_orphaned_storage(
    db: &DatabaseConnection,
    storage: &Backend,
    grace_period_hours: u64,
    now: DateTime<Utc>,
) -> Result<OrphanedStorageSummary, (OrphanedStorageSummary, anyhow::Error)> {
    let mut summary = OrphanedStorageSummary::default();
    // The inventory must complete before any deletion: a partial listing must
    // never be mistaken for evidence that an object is orphaned. Storage is
    // listed before the database so a folder created in between is authorized.
    let inventory = async {
        let stored = storage.list_storage_folders().await?;
        let locations = storage_location::Entity::find()
            .select_only()
            .column(storage_location::Column::FolderName)
            .into_tuple::<String>()
            .all(db)
            .await?;
        let uploads = upload::Entity::find()
            .select_only()
            .column(upload::Column::FolderName)
            .into_tuple::<String>()
            .all(db)
            .await?;
        Ok::<_, anyhow::Error>((stored, locations, uploads))
    };
    let (stored, locations, uploads) = match inventory.await {
        Ok(inventory) => inventory,
        Err(err) => return Err((summary, err)),
    };

    let authorized: HashSet<String> = locations.into_iter().chain(uploads).collect();
    let cutoff = now - chrono::Duration::hours(grace_period_hours as i64);
    summary.inspected_folders = stored.len() as u64;
    let mut orphaned = Vec::new();
    for folder in stored {
        if authorized.contains(&folder.folder_name) {
            summary.authorized_folders += 1;
        } else if folder.updated_at > cutoff {
            summary.grace_period_folders += 1;
        } else {
            orphaned.push(folder.folder_name);
        }
    }

    let results: Vec<Result<_, StorageError>> = futures::stream::iter(orphaned)
        .map(|folder| async move { storage.delete_folder(&folder).await })
        .buffer_unordered(5)
        .collect()
        .await;

    let mut first_error = None;
    for result in results {
        match result {
            Ok(deleted) => {
                summary.deleted_folders += 1;
                summary.deleted_objects += deleted.objects;
                summary.deleted_bytes += deleted.bytes;
            }
            Err(err) => {
                summary.failures += 1;
                first_error.get_or_insert(err);
            }
        }
    }
    match first_error {
        Some(err) => {
            let failures = summary.failures;
            Err((
                summary,
                anyhow::Error::new(err).context(format!(
                    "Failed to delete orphaned storage ({failures} failures)"
                )),
            ))
        }
        None => Ok(summary),
    }
}
