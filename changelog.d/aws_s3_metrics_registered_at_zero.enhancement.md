The `aws_s3` sink now registers `aws_s3_objects_delivered_total`, `aws_s3_objects_errored_total` and `aws_s3_delivery_errors_total` at zero when the sink is configured, so they are exported before the first increment. The `error_code`-labelled counters are registered without that label, since the codes are not known in advance.

authors: jamesdangercarpenter
