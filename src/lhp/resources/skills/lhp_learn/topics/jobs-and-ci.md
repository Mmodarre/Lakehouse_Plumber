# Topic — Dependencies, jobs, CI and pipeline wheels

Outcome: trace dependency information into deployment artifacts and a repeatable
build. Read [advanced](../references/advanced.md), [quickstart](../references/quickstart.md)
and [packaging configuration](../references/project-config.md). The sample's job
is hand-authored; do not claim it was created by dependency analysis. Pause each step.

## shipping.1 — Inspect dependencies

Run `lhp dag --format text` in the learning project. Trace one bronze-to-silver edge
back to produced/read table names. Explain pipeline-scoped views and external
sources; dynamic dependencies may require declarations. Activity: learner identifies
which upstream pipeline a gold result depends on. Pause.

## shipping.2 — Generate a separate job definition

Open `resources/sample_job.yml`, identifying the data-prep task that DAG-only job
generation cannot infer as pipeline work. Inspect `lhp dag --help` and the documented
job options. Let the learner create a small job configuration in a learning copy
using the actual supported settings and choose a distinct job name. Review their
edit and generate the job artifact without replacing the sample's hand-authored job.
Activity: compare its dependencies with the graph. Do not deploy. Pause.

## shipping.3 — Make the build repeatable

Have the learner outline a CI sequence: install a pinned LHP version, validate with
the environment/pipeline config, generate and review artifacts, then separately
validate/deploy a bundle under the team's controls. They write a small script or
adapt an existing workflow; inspect it and run only local checks. Explain secret
references and environment selection; do not create credentials, push or trigger
remote deployments as part of this exercise. Pause.

## shipping.4 — Inspect pipeline wheel packaging

Distinguish packaging generated pipelines from installing LHP itself. In a learning
copy, let the learner set `wheel.artifact_volume` to a labelled `/Volumes/...` value
and `packaging: wheel` for one pipeline in the pipeline config. These are the
pipeline runtime artifact rules; LHP agent setup still uses Workspace Files.
Validate and pause. On readiness generate and inspect the wheel, runner and bundle
resource dependencies. No upload is required to inspect these local artifacts.
Record the substep and explain runtime permissions separately. Pause.
