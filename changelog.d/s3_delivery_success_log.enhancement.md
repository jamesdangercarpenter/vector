The `aws_s3` sink (and S3-compatible sinks built on `s3_common`) now expose delivery-health observability:

- A rate-limited `info`-level `"Delivered object to S3-compatible storage."` log on successful writes, so log-based consumers get a positive delivery signal (previously only visible at `trace`).
- Hysteresis-based delivery-state transition logs — `"S3 delivery failing." error_code=<code>` after a run of consecutive failed writes, and `"S3 delivery recovered."` on recovery — plus an `aws_s3_delivery_up` gauge (1/0) and an `aws_s3_delivery_errors_total{error_code}` counter. The thresholds absorb transient/retried failures and concurrent partial failures so the signal doesn't flap.
- An opt-in `verify_write_permission` option that, at startup, writes a small marker object under the configured `key_prefix` to validate write permission (which the read-only `HeadBucket` healthcheck cannot detect).

authors: initvik
