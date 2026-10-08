# S3 storage returns alongside the filesystem

ADR-0010 dropped every storage backend but the filesystem. S3 is back as a second backend, selected with `STORAGE_DRIVER=s3`, because a shared bucket is the simplest way to run several replicas without a ReadWriteMany volume, and because direct downloads and Server-side Merge (ADR-0009) only exist on object storage. GCS stays dropped; its S3-compatible XML API can be used instead.

The storage engine talks to a `Backend` enum with one variant per backend rather than a trait object: there are two, both are known at compile time, and the engine's code paths stay identical for both. Each backend provides atomically visible writes (ADR-0004), listings that are complete or an error (ADR-0001), and folder deletion that reports what it removed. The filesystem additionally reports Filesystem Capacity; S3 additionally offers a Server-side Merge and presigned download URLs.

Behavior matches the TypeScript server's S3 adapter:

- Objects live under the `gh-actions-cache/` prefix, so a bucket can be shared, and a bucket left by the TypeScript server keeps its layout: its folders become Orphaned Storage after the first migration and are reclaimed after the grace period.
- A write of at most one part is a single `PutObject`; anything larger is a multipart upload that is aborted on failure and only becomes visible on completion. The expected length is checked before `PutObject`/`CompleteMultipartUpload`, so a truncated merge is never visible. A process killed mid-upload leaves an incomplete multipart upload that no listing shows; operators configure an `AbortIncompleteMultipartUpload` lifecycle rule.
- Eager Merge composes Parts with `UploadPartCopy` when they meet S3's multipart limits (ADR-0009).
- With `ENABLE_DIRECT_DOWNLOADS`, a merged entry's download URL is a presigned `GetObject` URL valid for 10 minutes. Issuing it is a Cache Access and takes a non-renewed Storage Reader Lease for the same 10 minutes, so cleanup cannot delete the object while the URL is usable.
- S3 has no capacity, so Capacity-based Eviction only applies with an explicit `CACHE_MAX_SIZE_BYTES` (ADR-0008).
- Path-style addressing is the default (`STORAGE_S3_FORCE_PATH_STYLE`), and credentials come from `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` or, without them, the AWS SDK's default chain (profile, web identity/IRSA, ECS, instance metadata).

The client is the official `aws-sdk-s3` with its HTTP client built on rustls with the `ring` provider, which keeps aws-lc, its C/CMake build and a second hyper/rustls generation out of the dependency tree. Request checksums are only sent where an operation requires them, because S3-compatible stores commonly reject the SDK's newer default checksums. `STORAGE_S3_SOCKET_TIMEOUT_MS` bounds the wait for a response and each gap in a download body; requests that carry or copy data get at least five minutes.

The whole integration suite runs against both backends: CI runs it once more with `TEST_STORAGE_DRIVER=s3` against a RustFS service, each test in its own bucket.
