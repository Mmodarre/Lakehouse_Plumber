# Lesson 6 — Transform and reuse

Prerequisite: a completed small edit. Outcome: explain the silver chain, distinguish
preset defaults from template structure, and follow the gold query. Read the
relevant transform leaves and [reuse](../references/templates-presets.md) only
when reaching that checkpoint. Each checkpoint ends with a pause.

## 6.1 — SQL changes the data

Open `pipelines/02_silver/orders_clean.yaml`. Resolve the local `entity` variable
and follow the bronze load into the SQL filter. Explain the fully qualified table
between pipelines and `stream(...)` for streaming SQL. Activity: locate the source,
filter predicate and output view. Pause.

## 6.2 — Schema changes affect downstream fields

Follow the schema action into `schema_transforms/orders_typed.yaml`. Trace
`o_totalprice` becoming `total_price` and compare the input/output views. Activity:
find that new name in `expectations/orders.yaml`. Explain the dependency; do not
teach every schema option. Pause.

## 6.3 — Quality rules and Python extensions

Read the quality action and its rule file, then the Python action and
`transforms/normalize_tz.py`. Explain their roles separately with a short example:
quality decides how violations are handled; Python applies custom transformation.
Activity: find the timezone parameter in substitutions and one rule's action.
If this is too much, split the inspection into two turns and record the substep.
Pause. CDC dimensions have their own optional lesson.

## 6.4 — Shared defaults

Open `presets/bronze_layer.yaml` and its use in `nation_region.yaml`. Explain action
matching and explicit overrides; trace one property to the existing generated
nation table. Activity: identify which setting came from the preset. Pause.

## 6.5 — Shared structure

Open `pipelines/01_ingest/orders_ingest.yaml`, then
`templates/cloudfiles_ingest.yaml`. Match parameters to the template's action
structure, then compare the lineitem instance. Activity: identify what changes
between JSON orders and CSV lineitems and what stays shared. Pause.

## 6.6 — Read the gold result

Open `pipelines/03_gold/sales_by_nation.yaml` and `sql/sales_by_nation.sql`.
Explain a materialized view as a maintained query result and trace the silver
inputs. Activity: connect one output column to its SQL expression. Recap the
source → transform → output story. Wait before [Lesson 7](07-deploy-and-observe.md),
which the learner may defer while completing local learning.
