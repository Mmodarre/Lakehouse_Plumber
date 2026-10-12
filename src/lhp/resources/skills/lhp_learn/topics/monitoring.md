# Topic — Monitoring and event logs

Outcome: explain the configured event-log path and generated monitoring resources.
Read [monitoring](../references/monitoring.md). Use the sample's existing setup;
wait at every checkpoint. Monitoring results require real pipeline execution.

## monitoring.1 — Find the event log

Open `lhp.yaml` and the generated ingestion pipeline resource. Trace event-log
catalog/schema/name. Explain per-pipeline overrides and that project metadata
columns serve a different purpose from event logs. Activity: learner finds one
pipeline's configured event-log destination. Pause.

## monitoring.2 — Follow consolidation

Read the sample's monitoring block, checkpoint path and job-config file. Locate the
generated consolidation code and summary views. Activity: trace one event log into
the monitoring output. Explain the required event-log configuration and checkpoint
storage; do not treat generated tables as already populated. Pause.

## monitoring.3 — Change a monitoring setting

In a learning copy, let the learner change `max_concurrent_streams` to a modest
positive value and predict the generated change. Inspect the saved edit, validate
and pause. After readiness generate and find the changed setting in the actual
monitoring artifact. Save the substep; do not deploy automatically. Pause.

## monitoring.4 — Read evidence

If requested and available, open actual pipeline event logs and monitoring outputs;
identify an update, a status and a data-quality metric. Otherwise inspect the query
that would compute them and label any illustrative event. Offer job monitoring or
a custom materialized view as a further learner-authored extension. Pause.
