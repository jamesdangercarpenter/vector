The `aws_s3` sink now emits an `aws_s3_delivery_errors_total` counter, labelled by `error_code`, incremented on every `PutObject` attempt that returns an error, including attempts that later succeed on retry. The label carries the S3 error code (such as `AccessDenied` or `SlowDown`), `Timeout` when `request.timeout_secs` elapsed, or the AWS SDK error variant for transport failures.

authors: jamesdangercarpenter
