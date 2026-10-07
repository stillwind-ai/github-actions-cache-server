use sea_orm_migration::prelude::*;

/// Tables written by the TypeScript server (Kysely migrations). Its schema is
/// incompatible, so a database it used is reset: cache data is disposable, and
/// the folders it leaves behind are reclaimed as Orphaned Storage.
const LEGACY_TABLES: &[&str] = &[
    "storage_reader_leases",
    "merge_leases",
    "cache_entries",
    "uploads",
    "storage_locations",
    "kysely_migration",
    "kysely_migration_lock",
];

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        let db = manager.get_connection();

        if manager.has_table("kysely_migration").await? {
            tracing::warn!(
                "Dropping the TypeScript server's tables; existing cache entries are discarded"
            );
            for table in LEGACY_TABLES {
                db.execute_unprepared(&format!(r#"DROP TABLE IF EXISTS "{table}" CASCADE"#))
                    .await?;
            }
        }

        // Keys use the "C" collation so the (repo_id, scope, version, key) index
        // also serves the `key LIKE 'prefix%'` restore-key lookups.
        db.execute_unprepared(
            r#"
            CREATE TABLE storage_locations (
                id                 uuid PRIMARY KEY,
                folder_name        text NOT NULL,
                part_count         integer NOT NULL CHECK (part_count > 0),
                size_bytes         bigint NOT NULL CHECK (size_bytes >= 0),
                created_at         timestamptz NOT NULL,
                merge_started_at   timestamptz,
                merged_at          timestamptz,
                parts_deleted_at   timestamptz,
                last_downloaded_at timestamptz
            );
            CREATE INDEX storage_locations_unmerged_parts_idx
                ON storage_locations (id)
                WHERE merged_at IS NOT NULL AND parts_deleted_at IS NULL;

            CREATE TABLE cache_entries (
                id          uuid PRIMARY KEY,
                repo_id     text NOT NULL,
                scope       text NOT NULL,
                version     text NOT NULL,
                key         text COLLATE "C" NOT NULL,
                updated_at  timestamptz NOT NULL,
                location_id uuid NOT NULL REFERENCES storage_locations (id) ON DELETE CASCADE
            );
            CREATE UNIQUE INDEX cache_entries_lookup_idx
                ON cache_entries (repo_id, scope, version, key);
            CREATE INDEX cache_entries_location_id_idx ON cache_entries (location_id);
            CREATE INDEX cache_entries_updated_at_idx ON cache_entries (updated_at);

            CREATE TABLE uploads (
                id                         bigint PRIMARY KEY,
                repo_id                    text NOT NULL,
                scope                      text NOT NULL,
                version                    text NOT NULL,
                key                        text COLLATE "C" NOT NULL,
                folder_name                text NOT NULL,
                created_at                 timestamptz NOT NULL,
                last_part_uploaded_at      timestamptz,
                started_part_upload_count  integer NOT NULL DEFAULT 0,
                finished_part_upload_count integer NOT NULL DEFAULT 0
            );
            CREATE UNIQUE INDEX uploads_lookup_idx ON uploads (repo_id, scope, version, key);

            CREATE TABLE merge_leases (
                storage_location_id uuid PRIMARY KEY
                    REFERENCES storage_locations (id) ON DELETE CASCADE,
                token               uuid NOT NULL,
                expires_at          timestamptz NOT NULL
            );

            CREATE TABLE storage_reader_leases (
                id                  uuid PRIMARY KEY,
                storage_location_id uuid NOT NULL
                    REFERENCES storage_locations (id) ON DELETE CASCADE,
                scope               text NOT NULL CHECK (scope IN ('parts', 'storage')),
                expires_at          timestamptz NOT NULL
            );
            CREATE INDEX storage_reader_leases_location_expiry_idx
                ON storage_reader_leases (storage_location_id, expires_at);
            "#,
        )
        .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .get_connection()
            .execute_unprepared(
                "DROP TABLE storage_reader_leases, merge_leases, uploads, cache_entries, storage_locations",
            )
            .await?;
        Ok(())
    }
}
