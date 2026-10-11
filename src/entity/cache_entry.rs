use sea_orm::entity::prelude::*;

/// A cache item available for matching and restoration by a workflow.
#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "cache_entries")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub id: Uuid,
    pub repo_id: String,
    pub scope: String,
    pub version: String,
    pub key: String,
    pub updated_at: DateTimeUtc,
    pub location_id: Uuid,
    #[sea_orm(belongs_to, from = "location_id", to = "id", on_delete = "Cascade")]
    pub location: HasOne<super::storage_location::Entity>,
}

impl ActiveModelBehavior for ActiveModel {}
