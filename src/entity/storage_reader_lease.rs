use sea_orm::entity::prelude::*;

/// What a Storage Reader Lease protects.
#[derive(Clone, Copy, Debug, PartialEq, Eq, EnumIter, DeriveActiveEnum)]
#[sea_orm(rs_type = "String", db_type = "Text")]
pub enum ReaderScope {
    /// A Part Reader Lease: the download reads the Storage Location's Parts.
    #[sea_orm(string_value = "parts")]
    Parts,
    /// The download reads the merged object.
    #[sea_orm(string_value = "storage")]
    Storage,
}

/// A time-bound claim that a download is actively reading a Storage Location.
#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "storage_reader_leases")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub id: Uuid,
    pub storage_location_id: Uuid,
    pub scope: ReaderScope,
    pub expires_at: DateTimeUtc,
    #[sea_orm(
        belongs_to,
        from = "storage_location_id",
        to = "id",
        on_delete = "Cascade"
    )]
    pub location: HasOne<super::storage_location::Entity>,
}

impl ActiveModelBehavior for ActiveModel {}
