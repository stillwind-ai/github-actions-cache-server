//! S3 storage. Objects live under the `gh-actions-cache/` prefix of the
//! configured bucket, which the server owns (ADR-0001): every top-level folder
//! below it is an upload/storage-location folder or Orphaned Storage. Anything
//! outside the prefix is left alone, so the bucket can be shared.
//!
//! Writes are atomically visible (ADR-0004): small objects are a single
//! `PutObject`, larger ones a multipart upload, which only appears on
//! completion. A crash mid-upload leaves an incomplete multipart upload that is
//! invisible to listings; an `AbortIncompleteMultipartUpload` lifecycle rule
//! reclaims it.

use std::collections::HashMap;
use std::io;
use std::time::Duration;

use aws_sdk_s3::Client;
use aws_sdk_s3::config::{
    BehaviorVersion, Credentials, Region, RequestChecksumCalculation, ResponseChecksumValidation,
    timeout::TimeoutConfig,
};
use aws_sdk_s3::error::{DisplayErrorContext, ProvideErrorMetadata, SdkError};
use aws_sdk_s3::presigning::PresigningConfig;
use aws_sdk_s3::primitives::ByteStream as S3ByteStream;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart, Delete, ObjectIdentifier};
use aws_smithy_http_client::tls;
use bytes::{Bytes, BytesMut};
use chrono::{DateTime, Utc};
use futures::{Stream, StreamExt};

use super::backend::{ComposeLimits, StorageDeletion, StorageError, StorageFolder, StorageObject};
use super::io::ByteStream;

/// Every key the server writes starts with this prefix and a slash.
const KEY_PREFIX: &str = "gh-actions-cache";

const MIB: u64 = 1024 * 1024;
/// S3 multipart limits: https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html
const MIN_PART_BYTES: u64 = 5 * MIB;
const MAX_PART_BYTES: u64 = 5 * 1024 * MIB;
const MAX_PARTS: usize = 10_000;
/// Size of the parts a streamed write is uploaded in, unless the expected
/// length needs larger ones to stay within `MAX_PARTS`.
const WRITE_PART_BYTES: u64 = 8 * MIB;
/// `DeleteObjects` takes at most this many keys.
const DELETE_BATCH: usize = 1000;

/// Requests that carry a body or copy data server-side can take far longer
/// than the socket timeout before the response arrives.
const MIN_TRANSFER_TIMEOUT: Duration = Duration::from_secs(5 * 60);

/// Connection settings, from `STORAGE_S3_*` and the usual `AWS_*` variables.
#[derive(Clone, Debug)]
pub struct S3Settings {
    pub bucket: String,
    pub region: String,
    pub endpoint_url: Option<String>,
    /// Without static credentials, the AWS SDK's default credential chain
    /// applies (environment, profile, web identity, ECS, instance metadata).
    pub access_key_id: Option<String>,
    pub secret_access_key: Option<String>,
    pub session_token: Option<String>,
    pub force_path_style: bool,
    /// Time allowed for a response, and between chunks of a download.
    pub socket_timeout: Duration,
}

impl S3Settings {
    fn transfer_timeout(&self) -> Duration {
        self.socket_timeout.max(MIN_TRANSFER_TIMEOUT)
    }
}

/// Builds an S3 client for these settings.
pub async fn client(settings: &S3Settings) -> Client {
    let http_client = aws_smithy_http_client::Builder::new()
        .tls_provider(tls::Provider::Rustls(
            tls::rustls_provider::CryptoMode::Ring,
        ))
        .build_https();
    let mut loader = aws_config::defaults(BehaviorVersion::latest())
        .region(Region::new(settings.region.clone()))
        .http_client(http_client)
        .timeout_config(
            TimeoutConfig::builder()
                .connect_timeout(Duration::from_secs(10))
                .read_timeout(settings.socket_timeout)
                .build(),
        );
    if let Some(endpoint_url) = &settings.endpoint_url {
        loader = loader.endpoint_url(endpoint_url);
    }
    if let (Some(access_key_id), Some(secret_access_key)) =
        (&settings.access_key_id, &settings.secret_access_key)
    {
        loader = loader.credentials_provider(Credentials::new(
            access_key_id,
            secret_access_key,
            settings.session_token.clone(),
            None,
            "environment",
        ));
    }
    let shared = loader.load().await;
    let config = aws_sdk_s3::config::Builder::from(&shared)
        .force_path_style(settings.force_path_style)
        // S3-compatible stores commonly reject the newer default checksums.
        .request_checksum_calculation(RequestChecksumCalculation::WhenRequired)
        .response_checksum_validation(ResponseChecksumValidation::WhenRequired)
        .build();
    Client::from_conf(config)
}

#[derive(Clone)]
pub struct S3Storage {
    client: Client,
    bucket: String,
    socket_timeout: Duration,
    transfer_timeout: Duration,
}

struct MultipartUpload {
    upload_id: String,
    parts: Vec<CompletedPart>,
}

fn s3_error<E, R>(err: SdkError<E, R>) -> StorageError
where
    E: std::error::Error + Send + Sync + 'static,
    R: std::fmt::Debug,
{
    StorageError::S3(DisplayErrorContext(err).to_string())
}

/// True for a 404 or a `NoSuchKey`/`NotFound` service error.
fn is_not_found<E>(err: &SdkError<E, aws_sdk_s3::config::http::HttpResponse>) -> bool
where
    E: ProvideErrorMetadata,
{
    if let Some(response) = err.raw_response()
        && response.status().as_u16() == 404
    {
        return true;
    }
    matches!(
        err.as_service_error().and_then(|err| err.code()),
        Some("NoSuchKey" | "NotFound")
    )
}

fn other(message: String) -> StorageError {
    StorageError::Io(io::Error::other(message))
}

impl S3Storage {
    pub const COMPOSE_LIMITS: ComposeLimits = ComposeLimits {
        min_part_bytes: MIN_PART_BYTES,
        max_part_bytes: MAX_PART_BYTES,
        max_parts: MAX_PARTS,
    };

    /// Connects and checks that the bucket exists.
    pub async fn new(settings: &S3Settings) -> Result<Self, StorageError> {
        let client = client(settings).await;
        if let Err(err) = client.head_bucket().bucket(&settings.bucket).send().await {
            if is_not_found(&err) {
                return Err(other(format!("Bucket {} does not exist", settings.bucket)));
            }
            return Err(s3_error(err));
        }
        Ok(Self {
            client,
            bucket: settings.bucket.clone(),
            socket_timeout: settings.socket_timeout,
            transfer_timeout: settings.transfer_timeout(),
        })
    }

    pub fn client(&self) -> &Client {
        &self.client
    }

    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    /// The key of an object name (`folder/parts/0`). Names are relative and
    /// slash-separated, without empty, `.` or `..` segments.
    pub fn key(&self, name: &str) -> Result<String, StorageError> {
        let valid = !name.is_empty()
            && name
                .split('/')
                .all(|segment| !matches!(segment, "" | "." | ".."));
        if !valid {
            return Err(StorageError::InvalidName(name.to_owned()));
        }
        Ok(format!("{KEY_PREFIX}/{name}"))
    }

    fn folder_prefix(&self, folder: &str) -> Result<String, StorageError> {
        Ok(format!("{}/", self.key(folder)?))
    }

    fn transfer_config(&self) -> aws_sdk_s3::config::Builder {
        aws_sdk_s3::config::Builder::default().timeout_config(
            TimeoutConfig::builder()
                .read_timeout(self.transfer_timeout)
                .build(),
        )
    }

    /// Streams an object, failing with [`StorageError::NotFound`] up front.
    /// A body that stalls for longer than the socket timeout fails.
    pub async fn read(&self, name: &str) -> Result<ByteStream, StorageError> {
        let response = match self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(self.key(name)?)
            .send()
            .await
        {
            Ok(response) => response,
            Err(err) if is_not_found(&err) => return Err(StorageError::NotFound(name.to_owned())),
            Err(err) => return Err(s3_error(err)),
        };
        let mut body = response.body;
        let timeout = self.socket_timeout;
        Ok(Box::pin(async_stream::stream! {
            loop {
                match tokio::time::timeout(timeout, body.next()).await {
                    Ok(Some(Ok(chunk))) => yield Ok(chunk),
                    Ok(Some(Err(err))) => {
                        yield Err(io::Error::other(err));
                        return;
                    }
                    Ok(None) => return,
                    Err(_) => {
                        yield Err(io::Error::new(
                            io::ErrorKind::TimedOut,
                            "S3 download stalled",
                        ));
                        return;
                    }
                }
            }
        }))
    }

    /// Writes an object with atomic visibility: one `PutObject` when the data
    /// fits in a single part, otherwise a multipart upload that is aborted on
    /// failure. With `expected_len`, a stream of any other length fails the
    /// write before the object becomes visible.
    pub async fn write<S>(
        &self,
        name: &str,
        stream: S,
        expected_len: Option<u64>,
    ) -> Result<u64, StorageError>
    where
        S: Stream<Item = io::Result<Bytes>> + Send + 'static,
    {
        let key = self.key(name)?;
        let mut multipart = None;
        let result = self
            .write_parts(&key, stream, expected_len, &mut multipart)
            .await;
        if result.is_err()
            && let Some(upload) = multipart
            && let Err(err) = self.abort_multipart_upload(&key, &upload.upload_id).await
        {
            tracing::warn!(key, error = %err, "Failed to abort S3 multipart upload");
        }
        result
    }

    async fn write_parts<S>(
        &self,
        key: &str,
        stream: S,
        expected_len: Option<u64>,
        multipart: &mut Option<MultipartUpload>,
    ) -> Result<u64, StorageError>
    where
        S: Stream<Item = io::Result<Bytes>> + Send + 'static,
    {
        let part_size = expected_len
            .map_or(WRITE_PART_BYTES, |len| {
                len.div_ceil(MAX_PARTS as u64).next_multiple_of(MIB)
            })
            .max(WRITE_PART_BYTES) as usize;
        let length_mismatch = |written: u64, expected: u64| {
            other(format!(
                "wrote {written} bytes to {key}, expected {expected}"
            ))
        };

        let mut stream = std::pin::pin!(stream);
        let mut buffer = BytesMut::new();
        let mut written = 0u64;
        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            written += chunk.len() as u64;
            if let Some(expected) = expected_len
                && written > expected
            {
                return Err(length_mismatch(written, expected));
            }
            buffer.extend_from_slice(&chunk);
            while buffer.len() >= part_size {
                let part = buffer.split_to(part_size).freeze();
                let upload = match multipart {
                    Some(upload) => upload,
                    None => multipart.insert(self.create_multipart_upload(key).await?),
                };
                self.upload_part(key, upload, part).await?;
            }
        }
        if let Some(expected) = expected_len
            && written != expected
        {
            return Err(length_mismatch(written, expected));
        }

        match multipart {
            None => {
                self.client
                    .put_object()
                    .bucket(&self.bucket)
                    .key(key)
                    .content_length(buffer.len() as i64)
                    .body(S3ByteStream::from(buffer.freeze()))
                    .customize()
                    .config_override(self.transfer_config())
                    .send()
                    .await
                    .map_err(s3_error)?;
            }
            Some(upload) => {
                if !buffer.is_empty() {
                    self.upload_part(key, upload, buffer.freeze()).await?;
                }
                self.complete_multipart_upload(key, upload).await?;
            }
        }
        Ok(written)
    }

    async fn create_multipart_upload(&self, key: &str) -> Result<MultipartUpload, StorageError> {
        let response = self
            .client
            .create_multipart_upload()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .map_err(s3_error)?;
        let upload_id = response
            .upload_id
            .ok_or_else(|| other("S3 did not return an UploadId".into()))?;
        Ok(MultipartUpload {
            upload_id,
            parts: Vec::new(),
        })
    }

    async fn upload_part(
        &self,
        key: &str,
        upload: &mut MultipartUpload,
        body: Bytes,
    ) -> Result<(), StorageError> {
        let part_number = upload.parts.len() as i32 + 1;
        let response = self
            .client
            .upload_part()
            .bucket(&self.bucket)
            .key(key)
            .upload_id(&upload.upload_id)
            .part_number(part_number)
            .content_length(body.len() as i64)
            .body(S3ByteStream::from(body))
            .customize()
            .config_override(self.transfer_config())
            .send()
            .await
            .map_err(s3_error)?;
        upload.parts.push(
            CompletedPart::builder()
                .part_number(part_number)
                .set_e_tag(response.e_tag)
                .build(),
        );
        Ok(())
    }

    async fn complete_multipart_upload(
        &self,
        key: &str,
        upload: &mut MultipartUpload,
    ) -> Result<(), StorageError> {
        self.client
            .complete_multipart_upload()
            .bucket(&self.bucket)
            .key(key)
            .upload_id(&upload.upload_id)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .set_parts(Some(std::mem::take(&mut upload.parts)))
                    .build(),
            )
            .customize()
            .config_override(self.transfer_config())
            .send()
            .await
            .map_err(s3_error)?;
        Ok(())
    }

    async fn abort_multipart_upload(&self, key: &str, upload_id: &str) -> Result<(), StorageError> {
        self.client
            .abort_multipart_upload()
            .bucket(&self.bucket)
            .key(key)
            .upload_id(upload_id)
            .send()
            .await
            .map_err(s3_error)?;
        Ok(())
    }

    /// Server-side Merge (ADR-0009): `CreateMultipartUpload`, one
    /// `UploadPartCopy` per Part, `CompleteMultipartUpload`.
    pub async fn compose_parts(&self, folder: &str, part_count: u32) -> Result<(), StorageError> {
        let key = self.key(&format!("{folder}/merged"))?;
        let mut upload = self.create_multipart_upload(&key).await?;
        let result = async {
            for index in 0..part_count {
                let source = self.key(&format!("{folder}/parts/{index}"))?;
                let part_number = index as i32 + 1;
                let copy = self
                    .client
                    .upload_part_copy()
                    .bucket(&self.bucket)
                    .key(&key)
                    .upload_id(&upload.upload_id)
                    .part_number(part_number)
                    .copy_source(format!("{}/{source}", self.bucket))
                    .customize()
                    .config_override(self.transfer_config())
                    .send()
                    .await
                    .map_err(s3_error)?;
                let e_tag = copy.copy_part_result.and_then(|result| result.e_tag);
                upload.parts.push(
                    CompletedPart::builder()
                        .part_number(part_number)
                        .set_e_tag(e_tag)
                        .build(),
                );
            }
            self.complete_multipart_upload(&key, &mut upload).await
        }
        .await;
        if result.is_err()
            && let Err(err) = self.abort_multipart_upload(&key, &upload.upload_id).await
        {
            tracing::warn!(key, error = %err, "Failed to abort S3 multipart upload");
        }
        result
    }

    pub async fn exists(&self, name: &str) -> Result<bool, StorageError> {
        match self
            .client
            .head_object()
            .bucket(&self.bucket)
            .key(self.key(name)?)
            .send()
            .await
        {
            Ok(_) => Ok(true),
            Err(err) if is_not_found(&err) => Ok(false),
            Err(err) => Err(s3_error(err)),
        }
    }

    /// Every object under `prefix`, or only those directly below it with
    /// `direct_children`. A listing is complete or an error, never partial.
    async fn list(
        &self,
        prefix: &str,
        direct_children: bool,
    ) -> Result<Vec<aws_sdk_s3::types::Object>, StorageError> {
        let mut objects = Vec::new();
        let mut continuation_token = None;
        loop {
            let mut request = self
                .client
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix(prefix)
                .set_continuation_token(continuation_token.take());
            if direct_children {
                request = request.delimiter("/");
            }
            let response = request.send().await.map_err(s3_error)?;
            objects.extend(response.contents.unwrap_or_default());
            if !response.is_truncated.unwrap_or(false) {
                return Ok(objects);
            }
            continuation_token = Some(response.next_continuation_token.ok_or_else(|| {
                other(format!(
                    "S3 listing for prefix \"{prefix}\" was truncated without a continuation token"
                ))
            })?);
        }
    }

    /// Deletes every object inside a folder, reporting what it contained.
    pub async fn delete_folder(&self, folder: &str) -> Result<StorageDeletion, StorageError> {
        let objects = self.list(&self.folder_prefix(folder)?, false).await?;
        let mut deleted = StorageDeletion::default();
        for batch in objects.chunks(DELETE_BATCH) {
            let identifiers = batch
                .iter()
                .filter_map(|object| object.key.as_deref())
                .map(|key| ObjectIdentifier::builder().key(key).build())
                .collect::<Result<Vec<_>, _>>()
                .map_err(|err| other(err.to_string()))?;
            let delete = Delete::builder()
                .set_objects(Some(identifiers))
                .quiet(true)
                .build()
                .map_err(|err| other(err.to_string()))?;
            let response = self
                .client
                .delete_objects()
                .bucket(&self.bucket)
                .delete(delete)
                .send()
                .await
                .map_err(s3_error)?;
            let errors = response.errors.unwrap_or_default();
            if !errors.is_empty() {
                let details = errors
                    .iter()
                    .map(|error| {
                        format!(
                            "{} ({})",
                            error.key.as_deref().unwrap_or("<unknown>"),
                            error.code.as_deref().unwrap_or("unknown error")
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                return Err(other(format!(
                    "S3 failed to delete {} object(s): {details}",
                    errors.len()
                )));
            }
            deleted.objects += batch.len() as u64;
            deleted.bytes += batch
                .iter()
                .map(|object| object.size.unwrap_or(0).max(0) as u64)
                .sum::<u64>();
        }
        Ok(deleted)
    }

    /// Objects directly inside a folder, names relative to it.
    pub async fn list_folder(&self, folder: &str) -> Result<Vec<StorageObject>, StorageError> {
        let prefix = self.folder_prefix(folder)?;
        Ok(self
            .list(&prefix, true)
            .await?
            .into_iter()
            .filter_map(|object| {
                let name = object.key?.strip_prefix(&prefix)?.to_owned();
                Some(StorageObject {
                    name,
                    bytes: object.size.unwrap_or(0).max(0) as u64,
                })
            })
            .collect())
    }

    /// Inventory of every top-level folder under the key prefix.
    pub async fn list_storage_folders(&self) -> Result<Vec<StorageFolder>, StorageError> {
        let prefix = format!("{KEY_PREFIX}/");
        let mut folders = HashMap::<String, StorageFolder>::new();
        for object in self.list(&prefix, false).await? {
            let Some(key) = object.key else { continue };
            let Some(folder_name) = key
                .strip_prefix(&prefix)
                .and_then(|name| name.split('/').next())
                .filter(|name| !name.is_empty())
            else {
                continue;
            };
            let modified = object
                .last_modified
                .and_then(|time| DateTime::<Utc>::from_timestamp(time.secs(), time.subsec_nanos()))
                .ok_or_else(|| {
                    other(format!(
                        "S3 did not return a modification time for object \"{key}\""
                    ))
                })?;
            let folder = folders
                .entry(folder_name.to_owned())
                .or_insert_with(|| StorageFolder {
                    folder_name: folder_name.to_owned(),
                    object_count: 0,
                    bytes: 0,
                    updated_at: DateTime::<Utc>::UNIX_EPOCH,
                });
            folder.object_count += 1;
            folder.bytes += object.size.unwrap_or(0).max(0) as u64;
            folder.updated_at = folder.updated_at.max(modified);
        }
        Ok(folders.into_values().collect())
    }

    /// A presigned `GetObject` URL.
    pub async fn download_url(
        &self,
        name: &str,
        expires_in: Duration,
    ) -> Result<String, StorageError> {
        let config = PresigningConfig::expires_in(expires_in.max(Duration::from_secs(1)))
            .map_err(|err| other(err.to_string()))?;
        let request = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(self.key(name)?)
            .presigned(config)
            .await
            .map_err(s3_error)?;
        Ok(request.uri().to_owned())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn keys_live_under_the_prefix_and_never_escape_it() {
        let storage = S3Storage {
            client: Client::from_conf(
                aws_sdk_s3::config::Builder::new()
                    .behavior_version(BehaviorVersion::latest())
                    .region(Region::new("us-east-1"))
                    .build(),
            ),
            bucket: "bucket".into(),
            socket_timeout: Duration::from_secs(1),
            transfer_timeout: Duration::from_secs(1),
        };
        assert_eq!(
            storage.key("123/parts/0").unwrap(),
            "gh-actions-cache/123/parts/0"
        );
        assert_eq!(
            storage.folder_prefix("123").unwrap(),
            "gh-actions-cache/123/"
        );
        for name in ["", "../x", "a/../../x", "/etc/passwd", "./x", "a//b", "a/"] {
            assert!(
                matches!(storage.key(name), Err(StorageError::InvalidName(_))),
                "{name}"
            );
        }
    }
}
