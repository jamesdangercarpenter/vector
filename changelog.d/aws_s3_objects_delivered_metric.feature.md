The `aws_s3` sink now emits an `aws_s3_objects_delivered_total` counter, incremented once per successful `PutObject`. This gives a per-object delivery count that `component_sent_events_total` (which counts events, not objects) cannot provide. The counter carries no bucket label since `component_id` already scopes it per sink, matching `aws_s3_delivery_errors_total`.

authors: jamesdangercarpenter
