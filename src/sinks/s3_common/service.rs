use std::{
    sync::{Arc, Mutex},
    task::{Context, Poll},
};

use aws_sdk_s3::{
    Client as S3Client, error::ProvideErrorMetadata, operation::put_object::PutObjectError,
};
use aws_smithy_runtime_api::client::{orchestrator::HttpResponse, result::SdkError};
use aws_smithy_types::byte_stream::ByteStream;
use base64::prelude::{BASE64_STANDARD, Engine as _};
use bytes::Bytes;
use futures::future::BoxFuture;
use md5::Digest;
use tower::Service;
use tracing::Instrument;
use vector_lib::{
    event::{EventFinalizers, EventStatus, Finalizable},
    request_metadata::{GroupedCountByteSize, MetaDescriptive, RequestMetadata},
    stream::DriverResponse,
};

use super::{config::S3Options, partitioner::S3PartitionKey};

#[derive(Debug, Clone)]
pub struct S3Request {
    pub body: Bytes,
    pub bucket: String,
    pub metadata: S3Metadata,
    pub request_metadata: RequestMetadata,
    pub content_encoding: Option<&'static str>,
    pub options: S3Options,
}

impl Finalizable for S3Request {
    fn take_finalizers(&mut self) -> EventFinalizers {
        std::mem::take(&mut self.metadata.finalizers)
    }
}

impl MetaDescriptive for S3Request {
    fn get_metadata(&self) -> &RequestMetadata {
        &self.request_metadata
    }

    fn metadata_mut(&mut self) -> &mut RequestMetadata {
        &mut self.request_metadata
    }
}

#[derive(Clone, Debug)]
pub struct S3Metadata {
    pub partition_key: S3PartitionKey,
    pub s3_key: String,
    pub finalizers: EventFinalizers,
}

#[derive(Debug)]
pub struct S3Response {
    events_byte_size: GroupedCountByteSize,
}

impl DriverResponse for S3Response {
    fn event_status(&self) -> EventStatus {
        EventStatus::Delivered
    }

    fn events_sent(&self) -> &GroupedCountByteSize {
        &self.events_byte_size
    }
}

// Delivery-health hysteresis. Report DOWN only after this many CONSECUTIVE failed
// writes and UP again after this many consecutive successes. Thresholds are what make
// the health signal correct under the real execution context: the tower Driver runs
// requests CONCURRENTLY and the retry layer sits BELOW this service, so a per-request
// edge would flap on interleaved outcomes and fire spurious "failing" on transient
// failures the retry then absorbs. Requiring a run of failures means only a sustained
// outage flips the badge; a snappy 1-success recovery keeps it responsive.
const FAILURE_THRESHOLD: u32 = 3;
const SUCCESS_THRESHOLD: u32 = 1;

/// Per-sink delivery health, guarded by a `Mutex` so a state transition and its side
/// effects (the edge log + the `delivery_up` gauge) form ONE critical section — the
/// only way to stay correct under the concurrent, out-of-order request completions the
/// Driver produces (an atomic swap + a separate `gauge.set` can otherwise interleave
/// and latch the gauge opposite to the real state). Starts UP (optimistic: idle =
/// healthy). The matching startup `delivery_up=1` gauge is published once, before any
/// request, from the sink run loop (see s3_common::sink).
struct DeliveryHealth {
    up: bool,
    consecutive_failures: u32,
    consecutive_successes: u32,
}

impl DeliveryHealth {
    const fn new() -> Self {
        Self {
            up: true,
            consecutive_failures: 0,
            consecutive_successes: 0,
        }
    }
}

/// Wrapper for the AWS SDK S3 client.
///
/// Provides a `tower::Service`-compatible wrapper around the native
/// AWS SDK S3 Client, allowing it to be composed within a Tower "stack",
/// such that we can easily and transparently provide retries, concurrency
/// limits, rate limits, and more.
#[derive(Clone)]
pub struct S3Service {
    client: S3Client,
    // Shared across the service's clones so all concurrent requests update one
    // health state and transitions are emitted once per real change.
    health: Arc<Mutex<DeliveryHealth>>,
}

impl S3Service {
    pub fn new(client: S3Client) -> S3Service {
        S3Service {
            client,
            health: Arc::new(Mutex::new(DeliveryHealth::new())),
        }
    }

    pub fn client(&self) -> S3Client {
        self.client.clone()
    }
}

/// Extracts a low-cardinality error label from a failed PutObject. Prefers the S3 API
/// error code (e.g. `InvalidAccessKeyId`, `AccessDenied`). For NON-service errors —
/// exactly the destination-down cases (timeout, connection/dispatch failure) where
/// `code()` is `None` — it falls back to a coarse class from the `SdkError` variant,
/// so a total outage is distinguishable from a permission error in the metric.
fn s3_error_code(error: &SdkError<PutObjectError, HttpResponse>) -> String {
    if let Some(code) = error.code() {
        return code.to_string();
    }
    match error {
        SdkError::TimeoutError(_) => "timeout",
        SdkError::DispatchFailure(_) => "dispatch_failure",
        SdkError::ResponseError(_) => "response_error",
        SdkError::ConstructionFailure(_) => "construction_failure",
        _ => "unknown",
    }
    .to_string()
}

impl Service<S3Request> for S3Service {
    type Response = S3Response;
    type Error = SdkError<PutObjectError, HttpResponse>;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    // Emission of an internal event in case of errors is handled upstream by the caller.
    fn poll_ready(&mut self, _cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    // Emission of internal events for errors and dropped events is handled upstream by the caller.
    fn call(&mut self, request: S3Request) -> Self::Future {
        let options = request.options;

        let content_encoding = request.content_encoding;
        let content_encoding = options
            .content_encoding
            .or_else(|| content_encoding.map(|ce| ce.to_string()));
        let content_type = options
            .content_type
            .or_else(|| Some("text/x-log".to_owned()));

        let content_md5 = BASE64_STANDARD.encode(md5::Md5::digest(&request.body));

        let tagging = options.tags.map(|tags| {
            let mut tagging = url::form_urlencoded::Serializer::new(String::new());
            for (p, v) in &tags {
                tagging.append_pair(p, v);
            }
            tagging.finish()
        });

        let events_byte_size = request
            .request_metadata
            .into_events_estimated_json_encoded_byte_size();

        let client = self.client.clone();
        let health = self.health.clone();

        Box::pin(async move {
            let put_request = client
                .put_object()
                .body(bytes_to_bytestream(request.body))
                .bucket(request.bucket.clone())
                .key(request.metadata.s3_key.clone())
                .set_content_encoding(content_encoding)
                .set_content_type(content_type)
                .set_acl(options.acl.map(Into::into))
                .set_grant_full_control(options.grant_full_control)
                .set_grant_read(options.grant_read)
                .set_grant_read_acp(options.grant_read_acp)
                .set_grant_write_acp(options.grant_write_acp)
                .set_server_side_encryption(options.server_side_encryption.map(Into::into))
                .set_ssekms_key_id(options.ssekms_key_id)
                .set_storage_class(Some(options.storage_class.into()))
                .set_tagging(tagging)
                .content_md5(content_md5);

            let result = put_request.send().in_current_span().await;

            match &result {
                Ok(_) => {
                    // A: rate-limited "still delivering" heartbeat so log-based
                    // consumers get a positive signal the metric-only success path
                    // (`component_sent_events_total`) can't give them. The per-object
                    // key is omitted so the rate limiter groups per sink (one periodic
                    // line while delivering) rather than per write (which would flood).
                    info!(
                        target: "vector::sinks::s3_common::service::put_object",
                        message = "Delivered object to S3-compatible storage.",
                        bucket = request.bucket,
                        internal_log_rate_limit = true,
                    );
                    trace!(
                        target: "vector::sinks::s3_common::service::put_object",
                        message = "Put object to s3-compatible storage.",
                        bucket = request.bucket,
                        key = request.metadata.s3_key,
                    );
                    // B/C: recovery is a hysteresis transition, not a per-request edge.
                    // One critical section owns the state change + its log + gauge, so
                    // concurrent completions can't interleave or latch the gauge wrong.
                    let mut h = health.lock().unwrap_or_else(|p| p.into_inner());
                    h.consecutive_failures = 0;
                    h.consecutive_successes = h.consecutive_successes.saturating_add(1);
                    if !h.up && h.consecutive_successes >= SUCCESS_THRESHOLD {
                        h.up = true;
                        info!(
                            message = "S3 delivery recovered.",
                            bucket = request.bucket,
                            internal_log_rate_limit = false,
                        );
                        #[allow(clippy::disallowed_macros)]
                        metrics::gauge!("aws_s3_delivery_up").set(1.0);
                    }
                }
                Err(error) => {
                    let error_code = s3_error_code(error);
                    // D: structured delivery-error counter labelled by S3 error code
                    // (component_id already scopes it per sink, so no bucket label).
                    #[allow(clippy::disallowed_macros)]
                    metrics::counter!(
                        "aws_s3_delivery_errors_total",
                        "error_code" => error_code.clone(),
                    )
                    .increment(1);
                    // B/C: only a RUN of failures (past the threshold) flips to failing —
                    // so a transient blip the retry layer below absorbs, or interleaved
                    // partial failures, don't flap the badge. One critical section.
                    let mut h = health.lock().unwrap_or_else(|p| p.into_inner());
                    h.consecutive_successes = 0;
                    h.consecutive_failures = h.consecutive_failures.saturating_add(1);
                    if h.up && h.consecutive_failures >= FAILURE_THRESHOLD {
                        h.up = false;
                        warn!(
                            message = "S3 delivery failing.",
                            bucket = request.bucket,
                            error_code = error_code.as_str(),
                            internal_log_rate_limit = false,
                        );
                        #[allow(clippy::disallowed_macros)]
                        metrics::gauge!("aws_s3_delivery_up").set(0.0);
                    }
                }
            }

            result.map(|_| S3Response { events_byte_size })
        })
    }
}

fn bytes_to_bytestream(buf: Bytes) -> ByteStream {
    ByteStream::from(buf)
}
