//! Storage lifecycle: merges, leases, eviction, cleanup tasks and self-healing.

mod common;

use std::time::Duration;

use cache_server::cleanup::Task;
use cache_server::entity::{
    cache_entry, merge_lease, storage_location, storage_reader_lease, upload,
};
use chrono::Utc;
use common::*;
use sea_orm::sea_query::Expr;
use sea_orm::{ColumnTrait, EntityTrait, PaginatorTrait, QueryFilter};

async fn location_of(server: &TestServer, key: &str) -> storage_location::Model {
    let entry = cache_entry::Entity::find()
        .filter(cache_entry::Column::Key.eq(key))
        .one(server.state.storage.db())
        .await
        .unwrap()
        .expect("cache entry");
    storage_location::Entity::find_by_id(entry.location_id)
        .one(server.state.storage.db())
        .await
        .unwrap()
        .unwrap()
}

async fn entry_count(server: &TestServer) -> u64 {
    cache_entry::Entity::find()
        .count(server.state.storage.db())
        .await
        .unwrap()
}

/// Reader leases are released by a task spawned when a stream is dropped.
async fn wait_for_no_reader_leases(server: &TestServer) {
    for _ in 0..100 {
        let leases = storage_reader_lease::Entity::find()
            .count(server.state.storage.db())
            .await
            .unwrap();
        if leases == 0 {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("reader leases were not released");
}

#[tokio::test(flavor = "multi_thread")]
async fn evicts_least_recently_used_entries_after_exceeding_the_budget() {
    let server = start_with(&[("CACHE_MAX_SIZE_BYTES", "20")]).await;
    server.save("a", "v1", &[1; 8], 1024).await;
    server.save("b", "v1", &[2; 8], 1024).await;
    // Accessing `a` makes `b` the least recently used.
    server.restore("a", "v1").await;
    wait_for_no_reader_leases(&server).await;

    // 24 bytes > 20: evict down to at most 18.
    server.save("c", "v1", &[3; 8], 1024).await;
    assert!(server.lookup("b", &[], "v1").await.is_none());
    assert_eq!(server.restore("a", "v1").await, [1; 8]);
    assert_eq!(server.restore("c", "v1").await, [3; 8]);
    assert_eq!(server.state.storage.total_stored_bytes().await.unwrap(), 16);
}

#[tokio::test(flavor = "multi_thread")]
async fn active_downloads_are_never_evicted() {
    const MB: usize = 1024 * 1024;
    let server = start_with(&[("CACHE_MAX_SIZE_BYTES", &(100 * MB).to_string())]).await;
    let data = random_bytes(64 * MB);
    server.save("reading", "v1", &data, 8 * MB).await;
    server.restore("reading", "v1").await;
    server.wait_for_merges().await;
    wait_for_no_reader_leases(&server).await;

    // Too large to fit in socket buffers: unread, the download stays active
    // and holds its Storage Reader Lease.
    let (url, _) = server.lookup("reading", &[], "v1").await.unwrap();
    let download = server.client.get(&url).send().await.unwrap();

    // Over budget, but the only older entry is being read, so the new
    // (oversized) one goes instead — its finalization still succeeds.
    server
        .save("new", "v1", &random_bytes(64 * MB), 8 * MB)
        .await;
    assert!(server.lookup("new", &[], "v1").await.is_none());
    assert_eq!(download.bytes().await.unwrap(), data);
    assert_eq!(server.restore("reading", "v1").await, data);
}

async fn hold_reader_lease(
    server: &TestServer,
    key: &str,
    scope: storage_reader_lease::ReaderScope,
) -> uuid::Uuid {
    let id = uuid::Uuid::new_v4();
    storage_reader_lease::Entity::insert(storage_reader_lease::ActiveModel {
        id: sea_orm::Set(id),
        storage_location_id: sea_orm::Set(location_of(server, key).await.id),
        scope: sea_orm::Set(scope),
        expires_at: sea_orm::Set(Utc::now() + chrono::Duration::minutes(2)),
    })
    .exec(server.state.storage.db())
    .await
    .unwrap();
    id
}

#[tokio::test(flavor = "multi_thread")]
async fn parts_are_deleted_after_the_merge_unless_a_part_reader_holds_them() {
    use storage_reader_lease::ReaderScope;

    let server = start().await;
    let data = random_bytes(3 * 1024 * 1024);
    server.save("parts", "v1", &data, 1024 * 1024).await;
    let summary = server.state.cleanup.run(Task::Parts).await.unwrap();
    assert_eq!(
        summary.deleted_parts,
        Some(0),
        "unmerged Parts are the only copy"
    );

    server.restore("parts", "v1").await;
    server.wait_for_merges().await;
    wait_for_no_reader_leases(&server).await;
    assert!(location_of(&server, "parts").await.merged_at.is_some());

    // A download that started before the merge completed still reads Parts.
    let part_reader = hold_reader_lease(&server, "parts", ReaderScope::Parts).await;
    hold_reader_lease(&server, "parts", ReaderScope::Storage).await;
    let summary = server.state.cleanup.run(Task::Parts).await.unwrap();
    assert_eq!(
        summary.deleted_parts,
        Some(0),
        "the Part Reader Lease protects the Parts"
    );

    storage_reader_lease::Entity::delete_by_id(part_reader)
        .exec(server.state.storage.db())
        .await
        .unwrap();
    // A merged-object reader doesn't hold the Parts.
    let summary = server.state.cleanup.run(Task::Parts).await.unwrap();
    assert_eq!(summary.deleted_parts, Some(3));
    let location = location_of(&server, "parts").await;
    assert!(location.parts_deleted_at.is_some());
    assert!(
        !server
            .storage_path()
            .join(&location.folder_name)
            .join("parts")
            .exists()
    );
    assert_eq!(server.restore("parts", "v1").await, data);

    // …but it does keep the whole Storage Location from being deleted.
    cache_entry::Entity::delete_many()
        .exec(server.state.storage.db())
        .await
        .unwrap();
    let summary = server
        .state
        .cleanup
        .run(Task::StorageLocations)
        .await
        .unwrap();
    assert_eq!(summary.deleted_locations, Some(0));
}

#[tokio::test(flavor = "multi_thread")]
async fn concurrent_first_downloads_all_get_the_payload() {
    let server = start().await;
    let data = random_bytes(5 * 1024 * 1024 + 17);
    server.save("concurrent", "v1", &data, 1024 * 1024).await;
    let (url, _) = server.lookup("concurrent", &[], "v1").await.unwrap();

    let downloads = (0..8).map(|_| server.download(&url));
    for (status, body) in futures::future::join_all(downloads).await {
        assert_eq!(status, 200);
        assert_eq!(body, data);
    }
    server.wait_for_merges().await;
    let merged = std::fs::read(
        server
            .storage_path()
            .join(location_of(&server, "concurrent").await.folder_name)
            .join("merged"),
    )
    .unwrap();
    assert_eq!(merged, data);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_client_aborting_the_first_download_does_not_abort_the_merge() {
    let server = start().await;
    let data = random_bytes(32 * 1024 * 1024);
    server.save("aborted", "v1", &data, 4 * 1024 * 1024).await;
    let (url, _) = server.lookup("aborted", &[], "v1").await.unwrap();

    let mut response = server.client.get(&url).send().await.unwrap();
    response.chunk().await.unwrap().expect("some data");
    drop(response);

    server.wait_for_merges().await;
    assert!(location_of(&server, "aborted").await.merged_at.is_some());
    assert_eq!(server.restore("aborted", "v1").await, data);
}

#[tokio::test(flavor = "multi_thread")]
async fn eager_merge_merges_at_upload_completion() {
    let server = start_with(&[("EAGER_MERGE", "true")]).await;
    let data = random_bytes(2 * 1024 * 1024 + 1);
    server.save("eager", "v1", &data, 1024 * 1024).await;
    server.wait_for_merges().await;

    let location = location_of(&server, "eager").await;
    assert!(location.merged_at.is_some());
    assert!(location.last_downloaded_at.is_none());
    let merged = std::fs::read(
        server
            .storage_path()
            .join(&location.folder_name)
            .join("merged"),
    )
    .unwrap();
    assert_eq!(merged, data);
    assert_eq!(server.restore("eager", "v1").await, data);
}

#[tokio::test(flavor = "multi_thread")]
async fn dangling_cache_entries_are_purged_and_matching_falls_back() {
    let server = start().await;
    server.save("deps-old", "v1", b"old", 1024).await;
    server.save("deps-new", "v1", b"new", 1024).await;

    // External mutation: the newest entry's data disappears.
    let folder = location_of(&server, "deps-new").await.folder_name;
    std::fs::remove_dir_all(server.storage_path().join(folder)).unwrap();

    let (url, matched) = server.lookup("deps-", &[], "v1").await.unwrap();
    assert_eq!(matched, "deps-old");
    assert_eq!(server.download(&url).await.1, b"old");
    assert_eq!(entry_count(&server).await, 1);
    assert_eq!(
        storage_location::Entity::find()
            .count(server.state.storage.db())
            .await
            .unwrap(),
        1
    );

    // An exact lookup of a dangling entry is a clean miss.
    let folder = location_of(&server, "deps-old").await.folder_name;
    std::fs::remove_dir_all(server.storage_path().join(folder)).unwrap();
    assert!(server.lookup("deps-old", &[], "v1").await.is_none());
    assert_eq!(entry_count(&server).await, 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn merged_entries_are_validated_by_their_merged_object() {
    let server = start().await;
    server.save("merged", "v1", b"payload", 1024).await;
    server.restore("merged", "v1").await;
    server.wait_for_merges().await;
    wait_for_no_reader_leases(&server).await;
    server.state.cleanup.run(Task::Parts).await.unwrap();

    let folder = location_of(&server, "merged").await.folder_name;
    std::fs::remove_file(server.storage_path().join(folder).join("merged")).unwrap();
    assert!(server.lookup("merged", &[], "v1").await.is_none());
    assert_eq!(entry_count(&server).await, 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn downloads_of_vanished_data_are_404s() {
    let server = start().await;
    server.save("vanishing", "v1", b"data", 1024).await;
    let (url, _) = server.lookup("vanishing", &[], "v1").await.unwrap();
    let folder = location_of(&server, "vanishing").await.folder_name;
    std::fs::remove_dir_all(server.storage_path().join(folder)).unwrap();

    assert_eq!(server.download(&url).await.0, 404);
    wait_for_no_reader_leases(&server).await;
    assert_eq!(
        server
            .download(&format!("{}/download/not-a-uuid", server.url))
            .await
            .0,
        404
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn reconciles_orphaned_storage_after_the_grace_period() {
    let server = start().await;
    server.save("authorized", "v1", b"kept", 1024).await;
    server.create_entry("in-progress", "v1").await.unwrap();

    let root = server.storage_path();
    let old = std::time::SystemTime::now() - Duration::from_secs(25 * 60 * 60);
    for (name, modified) in [("orphan-old", Some(old)), ("orphan-fresh", None)] {
        std::fs::create_dir_all(root.join(name).join("parts")).unwrap();
        let file = std::fs::File::create(root.join(name).join("parts/0")).unwrap();
        std::io::Write::write_all(&mut &file, b"12345").unwrap();
        if let Some(modified) = modified {
            file.set_modified(modified).unwrap();
            std::fs::File::open(root.join(name).join("parts"))
                .unwrap()
                .set_modified(modified)
                .unwrap();
            std::fs::File::open(root.join(name))
                .unwrap()
                .set_modified(modified)
                .unwrap();
        }
    }

    let summary = server
        .state
        .cleanup
        .run(Task::OrphanedStorage)
        .await
        .unwrap();
    let orphans = summary.orphaned_storage.unwrap();
    assert_eq!(orphans.inspected_folders, 3);
    assert_eq!(orphans.authorized_folders, 1);
    assert_eq!(orphans.grace_period_folders, 1);
    assert_eq!(
        (
            orphans.deleted_folders,
            orphans.deleted_objects,
            orphans.deleted_bytes
        ),
        (1, 1, 5)
    );
    assert!(!root.join("orphan-old").exists());
    assert!(root.join("orphan-fresh").exists());
    assert_eq!(server.restore("authorized", "v1").await, b"kept");
}

#[tokio::test(flavor = "multi_thread")]
async fn abandoned_uploads_are_cleaned_up() {
    let server = start().await;
    let upload_url = server.create_entry("abandoned", "v1").await.unwrap();
    server.upload_blocks(&upload_url, b"partial", 1024).await;
    server.create_entry("fresh", "v1").await.unwrap();

    let db = server.state.storage.db();
    let long_ago = Utc::now() - chrono::Duration::minutes(2);
    upload::Entity::update_many()
        .col_expr(upload::Column::CreatedAt, Expr::value(long_ago))
        .col_expr(upload::Column::LastPartUploadedAt, Expr::value(long_ago))
        .filter(upload::Column::Key.eq("abandoned"))
        .exec(db)
        .await
        .unwrap();

    let summary = server.state.cleanup.run(Task::Uploads).await.unwrap();
    assert_eq!(summary.deleted_uploads, Some(1));
    assert_eq!(summary.deleted_bytes, Some(7));
    assert_eq!(upload::Entity::find().count(db).await.unwrap(), 1);
    assert!(server.create_entry("abandoned", "v1").await.is_some());
}

#[tokio::test(flavor = "multi_thread")]
async fn expires_entries_after_the_retention_period() {
    let server = start_with(&[("CACHE_CLEANUP_OLDER_THAN_DAYS", "1")]).await;
    server.save("expired", "v1", b"old", 1024).await;
    server.save("recent", "v1", b"new", 1024).await;
    server.save("accessed", "v1", b"used", 1024).await;

    let db = server.state.storage.db();
    let two_days_ago = Utc::now() - chrono::Duration::days(2);
    cache_entry::Entity::update_many()
        .col_expr(cache_entry::Column::UpdatedAt, Expr::value(two_days_ago))
        .filter(cache_entry::Column::Key.is_in(["expired", "accessed"]))
        .exec(db)
        .await
        .unwrap();
    // Accessed recently, so retained despite its age.
    server.restore("accessed", "v1").await;
    wait_for_no_reader_leases(&server).await;

    let summary = server.state.cleanup.run(Task::CacheEntries).await.unwrap();
    assert_eq!(summary.deleted_locations, Some(1));
    assert!(server.lookup("expired", &[], "v1").await.is_none());
    assert!(server.lookup("recent", &[], "v1").await.is_some());
    assert!(server.lookup("accessed", &[], "v1").await.is_some());
}

#[tokio::test(flavor = "multi_thread")]
async fn resets_stalled_merges_and_purges_expired_leases() {
    let server = start().await;
    server.save("stalled", "v1", b"data", 1024).await;
    let location = location_of(&server, "stalled").await;
    let db = server.state.storage.db();
    let long_ago = Utc::now() - chrono::Duration::minutes(20);

    storage_location::Entity::update_many()
        .col_expr(
            storage_location::Column::MergeStartedAt,
            Expr::value(long_ago),
        )
        .filter(storage_location::Column::Id.eq(location.id))
        .exec(db)
        .await
        .unwrap();
    // A crashed merger's lease, long expired.
    merge_lease::Entity::insert(merge_lease::ActiveModel {
        storage_location_id: sea_orm::Set(location.id),
        token: sea_orm::Set(uuid::Uuid::new_v4()),
        expires_at: sea_orm::Set(long_ago),
    })
    .exec(db)
    .await
    .unwrap();

    let summary = server.state.cleanup.run(Task::Merges).await.unwrap();
    assert_eq!(summary.reset_merges, Some(1));
    assert_eq!(merge_lease::Entity::find().count(db).await.unwrap(), 0);
    assert!(
        location_of(&server, "stalled")
            .await
            .merge_started_at
            .is_none()
    );

    // The next download can take the Merge Lease and merge.
    assert_eq!(server.restore("stalled", "v1").await, b"data");
    server.wait_for_merges().await;
    assert!(location_of(&server, "stalled").await.merged_at.is_some());
}

#[tokio::test(flavor = "multi_thread")]
async fn all_cleanup_tasks_run_on_an_empty_database() {
    let server = start().await;
    for task in Task::ALL {
        server.state.cleanup.run(task).await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn cleanup_jobs_can_be_disabled() {
    let server = start_with(&[("DISABLE_CLEANUP_JOBS", "true")]).await;
    for task in Task::ALL {
        assert!(server.state.cleanup.run(task).await.unwrap().skipped);
    }
}
