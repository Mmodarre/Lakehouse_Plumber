# Lesson 1 — Meet your project

Prerequisite: Lesson 0 or an explicit skip. Outcome: locate the project's source
files and explain what the sample builds. Read [quickstart](../references/quickstart.md).
Use the learner's existing project when supplied; choose its smallest representative
flow for later lessons and record the path mapping. The paths below describe the
bundled sample, not every LHP project. Pause after every checkpoint.

## 1.1 — Put the project in reach

Explain the sample's TPC-H ingestion → silver → gold purpose and report the
persistent project folder selected during setup. If it is still empty apart from
learning progress, you may run `lhp init lhp_learning --sample --no-git` there.
`init` writes into the current directory, not a new child directory. Preserve
existing work; never initialize over `lhp.yaml`. If a project already exists,
inspect it instead. Read the generated README and show the learner how to open
this folder in their editor or Databricks Workspace Files.
Activity: the learner opens the project and locates `README.md`. Wait for readiness;
if they cannot see the files, resolve that access problem before continuing.

## 1.2 — Locate the files that matter first

Show the actual tree, focusing on `pipelines/`, `substitutions/`, `lhp.yaml` and
`config/`. Explain that `generated/` will be produced later. Introduce the existence
of `databricks.yml`, templates and presets without teaching all their fields.
Activity: ask the learner to find `pipelines/01_ingest/nation_region.yaml` and
`substitutions/dev.yaml`. Provide real links or navigable paths. Pause.

## 1.3 — Read the project story

Open the README's sample overview and locate ingestion, silver and gold folders.
Mention the data-prep notebook's role, but do not execute it or create remote
schemas, catalogs or volumes. The example catalog settings are placeholders for
local exploration until deployment choices are supplied.
Activity: have the learner trace which layer feeds the next in the folder tree.
Recap source files versus generated files, save progress and wait before
[Lesson 2](02-read-a-flowgroup.md).
