//! Merge Leases and Storage Reader Leases (ADR-0002).

use std::time::Duration;

use chrono::{DateTime, Utc};
use sea_orm::sea_query::{Expr, OnConflict};
use sea_orm::{
    ActiveValue::Set, ColumnTrait, ConnectionTrait, DbBackend, DbErr, EntityTrait, ExprTrait,
    QueryFilter, Statement,
};
use uuid::Uuid;

use crate::db::retry_on_lock_conflict;
use crate::entity::{merge_lease, storage_location, storage_reader_lease};

/// Expiring leases last two minutes and are renewed every 30 seconds.
pub const LEASE_DURATION: Duration = Duration::from_secs(2 * 60);
pub const LEASE_RENEWAL: Duration = Duration::from_secs(30);

fn lease_expiry(now: DateTime<Utc>) -> DateTime<Utc> {
    now + LEASE_DURATION
}

/// Takes the Merge Lease of a Storage Location unless another worker holds an
/// unexpired one. Returns the fencing token on success.
///
/// A single upsert: inserts a fresh lease, or takes over an expired one.
///
/// # Errors
///
/// If the database query fails.
pub async fn acquire_merge_lease(
    db: &impl ConnectionTrait,
    storage_location_id: Uuid,
) -> Result<Option<Uuid>, DbErr> {
    let token = Uuid::new_v4();
    let now = Utc::now();
    let rows = merge_lease::Entity::insert(merge_lease::ActiveModel {
        storage_location_id: Set(storage_location_id),
        token: Set(token),
        expires_at: Set(lease_expiry(now)),
    })
    .on_conflict(
        OnConflict::column(merge_lease::Column::StorageLocationId)
            .update_columns([merge_lease::Column::Token, merge_lease::Column::ExpiresAt])
            .action_and_where(
                Expr::col((merge_lease::Entity, merge_lease::Column::ExpiresAt)).lte(now),
            )
            .to_owned(),
    )
    .exec_without_returning(db)
    .await?;
    Ok((rows == 1).then_some(token))
}

/// # Errors
///
/// If the database query fails.
pub async fn renew_merge_lease(
    db: &impl ConnectionTrait,
    storage_location_id: Uuid,
    token: Uuid,
) -> Result<bool, DbErr> {
    let now = Utc::now();
    let result = merge_lease::Entity::update_many()
        .col_expr(
            merge_lease::Column::ExpiresAt,
            Expr::value(lease_expiry(now)),
        )
        .filter(merge_lease::Column::StorageLocationId.eq(storage_location_id))
        .filter(merge_lease::Column::Token.eq(token))
        .filter(merge_lease::Column::ExpiresAt.gt(now))
        .exec(db)
        .await?;
    Ok(result.rows_affected == 1)
}

/// # Errors
///
/// If the database query fails.
pub async fn release_merge_lease(
    db: &impl ConnectionTrait,
    storage_location_id: Uuid,
    token: Uuid,
) -> Result<(), DbErr> {
    merge_lease::Entity::delete_many()
        .filter(merge_lease::Column::StorageLocationId.eq(storage_location_id))
        .filter(merge_lease::Column::Token.eq(token))
        .exec(db)
        .await?;
    Ok(())
}

/// Starts a download of a Cache Entry: locks its Storage Location, takes a
/// Storage Reader Lease whose scope follows the merge state read under that
/// lock (ADR-0002), and records the Cache Access, all in one statement and so
/// one round trip. Returns the location as it was read and the lease, or
/// `None` when the entry doesn't exist.
///
/// # Errors
///
/// If the database fails.
pub async fn lease_for_download(
    db: &impl ConnectionTrait,
    cache_entry_id: Uuid,
) -> Result<Option<(storage_location::Model, Uuid)>, DbErr> {
    let lease_id = Uuid::new_v4();
    let now = Utc::now();
    let statement = Statement::from_sql_and_values(
        DbBackend::Postgres,
        r"
        WITH location AS (
            SELECT storage_locations.*
            FROM storage_locations
            JOIN cache_entries ON cache_entries.location_id = storage_locations.id
            WHERE cache_entries.id = $1
            FOR UPDATE OF storage_locations
        ), lease AS (
            INSERT INTO storage_reader_leases (id, storage_location_id, scope, expires_at)
            -- ReaderScope::Parts or ReaderScope::Storage.
            SELECT $2, id, CASE WHEN merged_at IS NULL THEN 'parts' ELSE 'storage' END, $3
            FROM location
        ), access AS (
            UPDATE storage_locations SET last_downloaded_at = $4
            FROM location
            WHERE storage_locations.id = location.id
        )
        SELECT * FROM location
        ",
        [
            cache_entry_id.into(),
            lease_id.into(),
            lease_expiry(now).into(),
            now.into(),
        ],
    );
    Ok(storage_location::Entity::find()
        .from_raw_sql(statement)
        .one(db)
        .await?
        .map(|location| (location, lease_id)))
}

/// # Errors
///
/// If the database query fails.
pub async fn renew_reader_lease(db: &impl ConnectionTrait, id: Uuid) -> Result<bool, DbErr> {
    let now = Utc::now();
    let result = storage_reader_lease::Entity::update_many()
        .col_expr(
            storage_reader_lease::Column::ExpiresAt,
            Expr::value(lease_expiry(now)),
        )
        .filter(storage_reader_lease::Column::Id.eq(id))
        .filter(storage_reader_lease::Column::ExpiresAt.gt(now))
        .exec(db)
        .await?;
    Ok(result.rows_affected == 1)
}

/// # Errors
///
/// If the database query fails.
pub async fn release_reader_lease(db: &impl ConnectionTrait, id: Uuid) -> Result<(), DbErr> {
    // Races cascade deletes of the storage location, which lock rows in the
    // opposite order.
    retry_on_lock_conflict(|| storage_reader_lease::Entity::delete_by_id(id).exec(db)).await?;
    Ok(())
}
