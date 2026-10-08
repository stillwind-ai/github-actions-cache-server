//! Storage backends: atomically visible writes and listings on whichever
//! backend the suite runs against, and the features only object storage has.

mod common;

use std::io;

use bytes::Bytes;
use cache_server::entity::{cache_entry, storage_location, storage_reader_lease};
use cache_server::storage::backend::StorageError;
use chrono::Utc;
use common::*;
use futures::StreamExt;
use sea_orm::{ColumnTrait, EntityTrait, QueryFilter};

const MB: usize = 1024 * 1024;

fn chunks(data: &[u8], size: usize) -> Vec<io::Result<Bytes>> {
    data.chunks(size)
        .map(|chunk| Ok(Bytes::copy_from_slice(chunk)))
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn writes_become_visible_whole_or_not_at_all() {
    let server = start().await;
    let backend = server.backend();

    // Larger than one S3 part, so it goes through a multipart upload.
    let data = random_bytes(17 * MB + 3);
    let written = backend
        .write(
            "1/merged",
            futures::stream::iter(chunks(&data, MB)),
            Some(data.len() as u64),
        )
        .await
        .unwrap();
    assert_eq!(written, data.len() as u64);
    assert!(server.read_object("1/merged").await.unwrap() == data);

    for (name, len) in [("2/merged", 3), ("3/merged", 17 * MB)] {
        let data = random_bytes(len);
        // Short by one byte: the write fails before anything is visible.
        let result = backend
            .write(
                name,
                futures::stream::iter(chunks(&data, MB)),
                Some(len as u64 + 1),
            )
            .await;
        assert!(result.is_err(), "{name}");
        // Long by one byte, too.
        let result = backend
            .write(
                name,
                futures::stream::iter(chunks(&data, MB)),
                Some(len as u64 - 1),
            )
            .await;
        assert!(result.is_err(), "{name}");
        assert!(!server.object_exists(name).await, "{name}");
    }

    // A stream failing after several parts leaves nothing behind either.
    let mut failing = chunks(&random_bytes(12 * MB), MB);
    failing.push(Err(io::Error::other("client went away")));
    assert!(
        backend
            .write("4/parts/0", futures::stream::iter(failing), None)
            .await
            .is_err()
    );
    assert!(!server.object_exists("4/parts/0").await);

    // Overwriting replaces the object.
    server.put_object("1/merged", b"replaced").await;
    assert_eq!(server.read_object("1/merged").await.unwrap(), b"replaced");

    let folders = server.storage_folders().await;
    assert_eq!(folders.len(), 1);
    assert_eq!(folders[0].folder_name, "1");
    assert_eq!((folders[0].object_count, folders[0].bytes), (1, 8));
}

#[tokio::test(flavor = "multi_thread")]
async fn lists_folders_and_deletes_them() {
    let server = start().await;
    let backend = server.backend();
    server.put_object("12/parts/0", b"hello").await;
    server.put_object("12/parts/1", b"world!").await;
    server.put_object("12/merged", b"helloworld!").await;
    // A folder sharing a name prefix is a different folder.
    server.put_object("123/parts/0", b"other").await;

    let mut parts = backend.list_folder("12/parts").await.unwrap();
    parts.sort_by(|a, b| a.name.cmp(&b.name));
    let parts: Vec<_> = parts.iter().map(|o| (o.name.as_str(), o.bytes)).collect();
    assert_eq!(parts, [("0", 5), ("1", 6)]);
    assert_eq!(backend.count_files("12/parts").await.unwrap(), 2);
    assert!(backend.list_folder("missing").await.unwrap().is_empty());

    let mut folders = server.storage_folders().await;
    folders.sort_by(|a, b| a.folder_name.cmp(&b.folder_name));
    let summary: Vec<_> = folders
        .iter()
        .map(|f| (f.folder_name.as_str(), f.object_count, f.bytes))
        .collect();
    assert_eq!(summary, [("12", 3, 22), ("123", 1, 5)]);
    assert!(folders[0].updated_at > Utc::now() - chrono::Duration::minutes(5));

    let deleted = backend.delete_folder("12/parts").await.unwrap();
    assert_eq!((deleted.objects, deleted.bytes), (2, 11));
    assert!(server.object_exists("12/merged").await);
    let deleted = backend.delete_folder("12").await.unwrap();
    assert_eq!((deleted.objects, deleted.bytes), (1, 11));
    let deleted = backend.delete_folder("12").await.unwrap();
    assert_eq!((deleted.objects, deleted.bytes), (0, 0));
    assert!(server.object_exists("123/parts/0").await);

    assert!(matches!(
        backend.read("12/merged").await,
        Err(StorageError::NotFound(_))
    ));
    for name in ["", "../x", "a/../../x", "/etc/passwd"] {
        assert!(
            matches!(
                backend.exists(name).await,
                Err(StorageError::InvalidName(_))
            ),
            "{name}"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn s3_listings_and_deletions_span_several_pages() {
    let server = start().await;
    if !server.is_s3() {
        return;
    }
    // S3 lists at most 1000 keys per page.
    let names: Vec<String> = (0..1001).map(|index| format!("paged/{index}")).collect();
    futures::stream::iter(&names)
        .for_each_concurrent(25, |name| server.put_object(name, b"x"))
        .await;

    let backend = server.backend();
    assert_eq!(backend.count_files("paged").await.unwrap(), 1001);
    let folders = server.storage_folders().await;
    assert_eq!(folders.len(), 1);
    assert_eq!(folders[0].object_count, 1001);

    let deleted = backend.delete_folder("paged").await.unwrap();
    assert_eq!((deleted.objects, deleted.bytes), (1001, 1001));
    assert_eq!(backend.count_files("paged").await.unwrap(), 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn s3_direct_downloads_hand_out_presigned_urls_for_merged_entries() {
    let server = start_with(&[("ENABLE_DIRECT_DOWNLOADS", "true")]).await;
    if !server.is_s3() {
        return;
    }
    let data = random_bytes(3 * MB);
    server.save("direct", "v1", &data, MB).await;

    // Unmerged: proxied, and the download merges.
    let (url, _) = server.lookup("direct", &[], "v1").await.unwrap();
    assert!(url.starts_with(&server.url), "{url}");
    assert!(server.download(&url).await.1 == data);
    server.wait_for_merges().await;

    let db = server.state.storage.db();
    let entry = cache_entry::Entity::find()
        .filter(cache_entry::Column::Key.eq("direct"))
        .one(db)
        .await
        .unwrap()
        .unwrap();
    // Wait for the proxied download's lease to go.
    for _ in 0..100 {
        let leases = storage_reader_lease::Entity::find()
            .filter(storage_reader_lease::Column::StorageLocationId.eq(entry.location_id))
            .all(db)
            .await
            .unwrap();
        if leases.is_empty() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }

    // Merged: straight from the bucket.
    let before = Utc::now();
    let (url, matched) = server.lookup("direct", &[], "v1").await.unwrap();
    assert_eq!(matched, "direct");
    assert!(!url.starts_with(&server.url), "{url}");
    assert!(url.contains("X-Amz-Signature="), "{url}");
    let response = reqwest::get(&url).await.unwrap();
    assert_eq!(response.status(), 200);
    assert!(response.bytes().await.unwrap() == data);

    // The URL counts as a Cache Access and holds a Storage Reader Lease for
    // its lifetime, which keeps the merged object from being deleted.
    let location = storage_location::Entity::find_by_id(entry.location_id)
        .one(db)
        .await
        .unwrap()
        .unwrap();
    assert!(location.last_downloaded_at.unwrap() >= before);
    let leases = storage_reader_lease::Entity::find()
        .filter(storage_reader_lease::Column::StorageLocationId.eq(entry.location_id))
        .all(db)
        .await
        .unwrap();
    assert_eq!(leases.len(), 1);
    assert_eq!(leases[0].scope, storage_reader_lease::ReaderScope::Storage);
    assert!(leases[0].expires_at > Utc::now() + chrono::Duration::minutes(9));

    cache_entry::Entity::delete_many().exec(db).await.unwrap();
    let summary = server
        .state
        .cleanup
        .run(cache_server::cleanup::Task::StorageLocations)
        .await
        .unwrap();
    assert_eq!(summary.deleted_locations, Some(0));
}

#[tokio::test(flavor = "multi_thread")]
async fn s3_storage_has_no_budget_without_an_explicit_maximum() {
    let server = start_with(&[("CACHE_FILESYSTEM_MAX_USAGE_PERCENT", "1")]).await;
    if !server.is_s3() {
        return;
    }
    server.save("a", "v1", &random_bytes(MB), MB).await;
    server.save("b", "v1", &random_bytes(MB), MB).await;
    let summary = server.state.storage.enforce_storage_budget().await.unwrap();
    assert_eq!(summary.evicted_locations, 0);
    assert!(server.lookup("a", &[], "v1").await.is_some());
}
