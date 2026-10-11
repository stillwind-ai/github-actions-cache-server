use sea_orm::entity::prelude::*;

/// A time-bound, fenced claim granting one worker authority to complete a Merge.
#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "merge_leases")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub storage_location_id: Uuid,
    pub token: Uuid,
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
