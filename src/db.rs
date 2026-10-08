use std::future::Future;
use std::time::Duration;

use sea_orm::{
    ConnectOptions, ConnectionTrait, Database, DatabaseConnection, DbErr, RuntimeErr,
    TransactionTrait,
};
use sea_orm_migration::MigratorTrait;

use crate::config::Config;
use crate::migration::Migrator;

pub async fn connect(config: &Config) -> Result<DatabaseConnection, DbErr> {
    let mut options = ConnectOptions::new(&config.database_url);
    options
        .max_connections(config.database_max_connections)
        .connect_timeout(Duration::from_secs(10))
        .acquire_timeout(Duration::from_secs(30))
        .sqlx_logging(config.debug)
        .sqlx_logging_level(tracing::log::LevelFilter::Debug);
    Database::connect(options).await
}

/// Applies pending migrations. Replicas starting together serialize on an
/// advisory lock; Postgres DDL is transactional, so the whole run is atomic.
pub async fn migrate(db: &DatabaseConnection) -> Result<(), DbErr> {
    let txn = db.begin().await?;
    txn.execute_unprepared(
        "SELECT pg_advisory_xact_lock(hashtext('github-actions-cache-server:migrate'))",
    )
    .await?;
    Migrator::up(&txn, None).await?;
    txn.commit().await
}

/// Postgres `serialization_failure` and `deadlock_detected`.
const RETRYABLE_SQLSTATES: &[&str] = &["40001", "40P01"];

pub fn is_retryable_lock_error(err: &DbErr) -> bool {
    let (DbErr::Exec(RuntimeErr::SqlxError(err))
    | DbErr::Query(RuntimeErr::SqlxError(err))
    | DbErr::Conn(RuntimeErr::SqlxError(err))) = err
    else {
        return false;
    };
    err.as_database_error()
        .and_then(|err| err.code())
        .is_some_and(|code| RETRYABLE_SQLSTATES.contains(&code.as_ref()))
}

/// Runs a transaction, retrying it whole if the database picks it as a
/// deadlock victim. Concurrent writers take row locks on `storage_locations`
/// and the lease tables in differing orders, so a deadlock is expected rather
/// than exceptional — the loser has to start over. Only wrap transactions that
/// are safe to repeat.
pub async fn retry_on_lock_conflict<T, F, Fut>(mut run: F) -> Result<T, DbErr>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, DbErr>>,
{
    const ATTEMPTS: u32 = 3;
    let mut attempt = 1;
    loop {
        match run().await {
            Err(err) if attempt < ATTEMPTS && is_retryable_lock_error(&err) => {
                tracing::warn!(attempt, error = %err, "Retrying transaction after lock conflict");
                attempt += 1;
            }
            result => return result,
        }
    }
}
