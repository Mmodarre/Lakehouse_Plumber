# Lesson 7 — Deploy and observe (optional execution)

Prerequisite: generated sample and an explicit decision to execute it. Outcome:
distinguish generated assets, deployed resources and observed runtime behaviour.
Read the project's actual README and [deployment](../references/advanced.md).
Pause at every checkpoint. A learner choosing 'ready' for an explanation has not
implicitly requested deployment, a job run or destruction of existing resources.

## 7.1 — Inspect what will run

Open `resources/sample_job.yml`, `notebooks/data_prep.py` and `databricks.yml` in
small excerpts. Trace data preparation before ingest → silver → gold. Explain the
actual resources to create and compute to use. Ask whether they want to execute
in their development workspace or defer execution; save a defer without marking
runtime verification complete. Continue locally to Lesson 8 when they choose it.

## 7.2 — Configure the real destination

If executing, discover available authenticated tools and collect only missing
workspace/catalog choices. Have the learner set their development workspace host,
bundle catalog variable, substitution catalog and matching landing-volume path.
Check permissions and the sample's UC/serverless/source availability; do not assume
all workspaces expose the sample dataset. Never ask for credentials in chat.
Activity: learner reviews the destination and their saved configuration. Pause.

## 7.3 — Validate and deploy

Revalidate/regenerate for the real environment. Run bundle validation using the
available authenticated Databricks tooling. Show its destination and resources.
Pause for the learner's decision to deploy. On that decision, deploy the sample
bundle (normally `databricks bundle deploy --target dev`), inspect the real result
and point them to the actual workspace resources. End the response and wait before
any job execution. Record validation and deployment as separate substeps.

## 7.4 — Run and inspect

On the learner's decision to run, execute the actual sample job (normally
`databricks bundle run sample_job --target dev`). Inspect task outcomes. Help the
learner open the job and generated pipelines, then query bronze, silver and gold
results with the actual configured catalog. Activity: learner inspects one result
and links it to its source definition. Never supply made-up counts. Pause.

## 7.5 — Observe a second update

Explain that rerunning data preparation advances the demo's data. If the learner
chooses another run, inspect its actual changes and distinguish incremental ingest,
CDC history and refreshed gold results. Activity: compare observed results; report
uncertainty if metrics/data are unavailable. Discuss cleanup choices without
executing destructive cleanup automatically. Save evidence and wait before
[Lesson 8](08-build-your-own.md).
