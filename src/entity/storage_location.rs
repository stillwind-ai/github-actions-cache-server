use sea_orm::entity::prelude::*;

/// The stored data belonging to a Cache Entry: its Parts and, once merged, the
/// merged object, all under `folder_name`.
#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "storage_locations")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub id: Uuid,
    pub folder_name: String,
    pub part_count: i32,
    /// Bytes of the finalized payload (sum of its Parts).
    pub size_bytes: i64,
    pub created_at: DateTimeUtc,
    pub merge_started_at: Option<DateTimeUtc>,
    pub merged_at: Option<DateTimeUtc>,
    pub parts_deleted_at: Option<DateTimeUtc>,
    /// Most recent Cache Access.
    pub last_downloaded_at: Option<DateTimeUtc>,
    #[sea_orm(has_many)]
    pub cache_entries: HasMany<super::cache_entry::Entity>,
    #[sea_orm(has_many)]
    pub reader_leases: HasMany<super::storage_reader_lease::Entity>,
    #[sea_orm(has_one)]
    pub merge_lease: HasOne<super::merge_lease::Entity>,
}

impl ActiveModelBehavior for ActiveModel {}
