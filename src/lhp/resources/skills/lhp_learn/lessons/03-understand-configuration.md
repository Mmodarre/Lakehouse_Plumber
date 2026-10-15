# Lesson 3 — Understand configuration

Prerequisite: one traced flowgroup. Outcome: know which of four files owns a
setting and trace an environment token. Read [project configuration](../references/project-config.md)
and [quickstart](../references/quickstart.md). Use one file per checkpoint and wait.

## 3.1 — Project-wide settings

Open `lhp.yaml`; identify the project name. Explain the sample's metadata, event-log
and monitoring sections by purpose only. Defining a metadata column here is
separate from selecting it on an action/flowgroup. Catalog/schema for pipeline
resources belong in the pipeline configuration, not invented `lhp.yaml` fields.
Activity: locate the project name and one shared metadata definition. Pause.

## 3.2 — Environment values

Open `substitutions/dev.yaml`; inspect the `dev:` mapping and resolve `${catalog}`
and `${bronze_schema}` in the nation write from Lesson 2. Open `prod.yaml` only to
show that values can differ while the flowgroup stays the same. Secret references
name runtime lookups; never put literal credentials in this exercise.
Activity: the learner finds the resolved destination using the actual mappings.
Keep sample values labelled examples, and ask for real values only for a requested
deployment. Pause.

## 3.3 — Pipeline runtime configuration

Open `config/pipeline_config.yaml`. Explain `project_defaults`, the `---` documents,
per-pipeline overrides and where catalog, schema and compute settings come from.
Show why bundle-enabled validation/generation needs `-pc config/pipeline_config.yaml`.
Activity: find the silver schema override and the setting it inherits. Pause.

## 3.4 — Deployment configuration

Open `databricks.yml`. Locate the development target, workspace host, catalog
variable, includes and generated-file sync. The bundle's catalog variable feeds
data preparation; LHP's catalog substitution feeds generation. They must agree
before execution, and the landing-volume path must match the chosen catalog.
Activity: point to the two catalog settings; no deployment or edits are needed
just to understand them. Pause.

## 3.5 — Predict an environment change

Give a small scenario: the same pipeline will write into a different development
catalog. Ask the learner to name the settings they would change, including the
sample's landing path and bundle variable. Give feedback using their actual files.
If they want to configure a real workspace now, let them edit the supplied values
and review the diff. Otherwise keep the exercise explanatory with labelled sample
values. Save progress and wait before [Lesson 4](04-generate-and-inspect.md).
