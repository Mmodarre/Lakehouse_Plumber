# Topic — Publish test results

Prerequisite: one configured data test. Outcome: trace an assertion into a reporting
provider and distinguish generated wiring from a successful publication. Read
[test reporting](../references/test-reporting.md). Pause after each checkpoint.

## reporting.1 — Follow one result

Explain stable `test_id`, status/counts and update identity using a labelled example.
Read the provider signature `publish_results(results, config, context, spark)`.
Activity: learner identifies what uniquely describes the test and execution. Pause.

## reporting.2 — Wire the provider

Use an isolated project copy. Let the learner add a stable ID to their test, select
`on_violation: warn` for reported metrics, and configure `test_reporting` in
`lhp.yaml` using the documented module/function/config-file fields. The local
reference contains a small Delta provider example they may adapt themselves.
Use a labelled target for local generation; do not claim destination access.
Activity: learner saves the provider and configuration. Pause.

## reporting.3 — Inspect the generated hook

Review the edit and validate with `--include-tests` and the bundle pipeline config.
Pause. On readiness generate and locate the copied provider, configured arguments
and generated event hook. Activity: trace their test ID into the reporting path.
Generation does not publish results. Save the substep and pause.

## reporting.4 — Consider execution and retries

Discuss permissions, connectivity and provider-specific credentials only for a
requested runtime exercise. Explain that the example's append-only Delta write
needs deduplication by execution/test identity if retries must not duplicate rows.
Have the learner choose how they would handle one repeated event. If they opt to
execute, inspect actual published results separately. Return to the course on
request and retain progress. Pause.
