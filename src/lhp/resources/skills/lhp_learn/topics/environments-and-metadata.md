# Topic — Environments, secrets and metadata

Outcome: separate reusable logic from environment values and attach meaningful
metadata. Read [configuration](../references/project-config.md),
[reuse](../references/templates-presets.md) and [UC tags](../references/uc-tags.md).
Pause at every checkpoint; begin at the requested subtopic if prerequisites exist.

## config.1 — Trace the kinds of values

Use an actual environment token, local variable and template parameter from the
sample. Explain `${token}`, `%{local_var}` and `{{ parameter }}` through those
locations. Secret references become runtime lookups rather than literal secrets.
Activity: learner identifies the scope of one value. Pause.

## config.2 — Make an environment-specific change

Let the learner create a separate learning environment file based on `dev.yaml`,
renaming its top-level environment mapping and choosing labelled catalog/schema
values. For a bundle, have them add a matching target using the existing dev target
as a guide, without deploying it. Inspect their attempt and validate that environment
with its pipeline config. Pause. After readiness generate and compare its resolved
names with dev; preserve the earlier output for comparison. Save the substep.

## config.3 — Select an operational column

Open metadata definitions in `lhp.yaml` and one flowgroup/action selection. Explain
where file metadata is available versus a processing timestamp on non-file data.
Let the learner opt an appropriate action into an existing supported column. Review,
validate and pause; then generate and locate the expression after readiness.
Do not put `operational_metadata` inside a write target. Pause.

## config.4 — Tag a supported target

Read the local UC-tag contract. Have the learner add one illustrative table tag in
a learning copy using the supported write fields, or a tag file for table/column
metadata. Validate, pause, generate and inspect emitted tagging code. Explain that
actual tag changes require runtime permissions and execution. Save substep and pause.

## config.5 — Inspect a secret reference

Use the optional sample JDBC example if present, keeping it disabled until selected
for a complete exercise. Trace scope alias → secret reference → generated lookup.
The learner can add a reference using placeholder scope/key names for generation;
never fetch or print secret values for this lesson. Pause before any runtime use.
