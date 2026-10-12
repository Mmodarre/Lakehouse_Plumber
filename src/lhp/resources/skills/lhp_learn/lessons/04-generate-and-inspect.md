# Lesson 4 — Validate, generate and inspect

Prerequisite: the four configuration roles. Outcome: map one YAML action to Python
and distinguish checks from execution. Use the selected installed interpreter/CLI
from setup and the actual project root. Pause after each checkpoint.

## 4.1 — Validate the configuration

Explain the check, then run `lhp validate --env dev -pc config/pipeline_config.yaml`
for the bundled sample. For another project, choose its actual environment and
include `-pc` when bundle-enabled. Read the real result and any warning together.
Use [errors](../references/errors.md) for failures. Let the learner repair a relevant
configuration mistake; do not erase their attempt without explaining it.
Activity: inspect the output and distinguish checked configuration from run data.
Pause; do not generate in this checkpoint.

## 4.2 — Generate code

Explain that generation writes the Python and bundle resources, then run
`lhp generate --env dev -pc config/pipeline_config.yaml` for the sample. Locate the
actual outputs; do not promise a fixed file count. Generation is a full regenerate,
so maintained edits belong in the source YAML. Test actions are omitted unless
`--include-tests` is selected; their separate lesson covers that option.
Activity: open `generated/dev/` and locate the generated `nation_region` file.
Pause before explaining its internals.

## 4.3 — Match an action to Python

Open the generated file next to the source YAML. Trace the nation's Delta read,
view declaration and streaming-table write into actual generated constructs.
Show where `${catalog}` resolved; do not invent function names or decorators.
Activity: find the generated reference corresponding to `v_nation` and the final
table. Explain any preset-added properties only as inherited settings. Pause.

## 4.4 — Inspect the deployment resource

Open the relevant generated file under `resources/lhp/`. Trace its pipeline name,
catalog/schema and Python library paths back to configuration. These are generated
resources; no pipeline has been deployed merely because the files exist.
Activity: locate one runtime setting and its source configuration. Save progress
and wait before [Lesson 5](05-make-a-change.md).
