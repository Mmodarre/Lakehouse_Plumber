# Topic — Presets, templates and blueprints

Outcome: choose a reuse tool based on what repeats. Read [templates/presets](../references/templates-presets.md),
[blueprints](../references/blueprints.md) and [project configuration](../references/project-config.md).
Use sample artifacts where present and an isolated learning copy for experiments.
Pause after each checkpoint; a learner can request only one of the three tools.

## reuse.1 — Recognise the repetition

Compare `nation_region.yaml` and `presets/bronze_layer.yaml`, then the orders and
lineitem template instances. Explain shared defaults versus shared action shape.
For tenant/region repetitions, introduce a blueprint as a set of flowgroups.
Activity: match a repeated setting, repeated ingestion shape and regional pipeline
to the appropriate tool. Pause.

## reuse.2 — Trace a preset

Locate one property in `bronze_layer.yaml` and its generated write. Explain matching
by action type/subtype and explicit overrides. Have the learner make a small
comment/property override on one appropriate action in the learning copy. They
predict whether other users of the preset change, save, then you validate. Pause.
After readiness generate and inspect the specific overridden output. Save substep.

## reuse.3 — Instantiate a template

Open `templates/cloudfiles_ingest.yaml` and one instance. Map required/defaulted
parameters to the action shape. Let the learner author another small instance with
distinct flowgroup/view/output names and a labelled example source path, using the
actual parameter declarations. Check their attempt; validate and pause before
generation. Inspect the expansion together after readiness. Do not invent a
parameter just because its name sounds useful.

## reuse.4 — Expand a blueprint

Open the local blueprint reference's complete example. Have the learner create a
small blueprint in a learning copy and two instance files under `pipelines/`, with
different `site_name` values. Explain `%{...}` identifiers and `${...}` environment
values. Activity: predict the number and names of expanded flowgroups. Pause after
the learner saves; do not write the whole answer unless asked to demonstrate.

## reuse.5 — Inspect expansion and organisation

Run `lhp list blueprints --instances` and inspect the discovered names. Validate,
then pause before generation. Inspect the two generated results after readiness.
Explain multi-document flowgroup YAML as a file organisation choice, independent
of the reuse tool. Reflect on which repeated element each tool removed. Save and
pause before returning to the course.
