The `aws_s3` sink's `aws_s3_delivery_errors_total` and `aws_s3_objects_errored_total` counters no longer carry an `error_code` label, so each is a single per-component series that exists from startup. The per-error-code breakdown moved to two new counters, `aws_s3_delivery_errors_count` and `aws_s3_objects_errored_count`, which carry the `error_code` label and appear as each code is first seen.

authors: jamesdangercarpenter
