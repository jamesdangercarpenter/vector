The `aws_s3` sink now emits an `aws_s3_objects_errored_total` counter, labelled by `error_code`, incremented once per object whose `PutObject` returned an error at least once. It fires on the object's first error and not on retries, so it counts objects that hit trouble, where `aws_s3_delivery_errors_total` counts every errored attempt.

authors: jamesdangercarpenter
