# Developer Sandbox Mode (`--sandbox`)

Gives each developer a personal, namespaced copy of THEIR slice of the project
without touching shared tables. Scope comes from a personal gitignored
`.lhp/profile.yaml`; only those pipelines generate, and every table they PRODUCE
is renamed into the developer's namespace at the table leaf (catalog/schema
unchanged). Core invariant: **read-shared / write-own** — produced tables are
renamed on the write AND on every in-scope read of them; reads of out-of-scope
shared tables stay pointed at the shared tables.

## Activation

```bash
lhp generate -e dev --sandbox    # scoped generate, tables namespaced
lhp validate -e dev --sandbox    # same scoping + structured renames
```

- Available on `lhp generate` and `lhp validate` only (boolean flag).
- **Mutually exclusive with `-p`/`--pipeline`** — combining them is a usage error
  (exit code 2): `--sandbox cannot be combined with -p/--pipeline: sandbox scope
  comes from .lhp/profile.yaml`.
- Works with `--dry-run`.
- `--strict` promotes the sandbox warnings (`VAL-065`/`VAL-066`) to failures.

## `.lhp/profile.yaml` (personal, gitignored)

Lives at `<project_root>/.lhp/profile.yaml`; the `.lhp/` directory is gitignored.
Explicit opt-in — namespace and scope are never auto-detected. Payload nests
under a top-level `sandbox:` key. Missing file → `LHP-IO-025`; invalid file
(bad YAML, non-mapping root, missing `sandbox:` key, failed validation) →
`LHP-CFG-064`.

```yaml
# .lhp/profile.yaml
sandbox:
  namespace: alice
  pipelines:
    - bronze_*          # case-sensitive fnmatchcase glob
    - silver_orders     # exact pipeline name
```

| key | type | required | notes |
|-----|------|----------|-------|
| `namespace` | str | yes | Regex `^[a-z][a-z0-9_]{0,63}$` — lowercase letter first, then lowercase letters/digits/underscores, max 64 chars total. |
| `pipelines` | list[str] | yes | Non-empty; exact pipeline names or case-sensitive `fnmatchcase` globs (an entry is a glob iff it contains `*` `?` `[`). Entry matching zero pipelines → `LHP-VAL-064` (all offenders aggregated into one error). |

## `lhp.yaml` `sandbox:` block (team policy, optional)

Committed team policy. When the block is absent, defaults apply: strategy
`table`, pattern `{namespace}_{table}`, any env allowed.

| key | type | default | notes |
|-----|------|---------|-------|
| `strategy` | `table` | `table` | v1 supports `table` only (schema/catalog strategies reserved). Any other value → `LHP-CFG-062`. |
| `table_pattern` | str | `{namespace}_{table}` | Formats the table LEAF only. Validated like `str.format`: placeholders must be a subset of `{namespace, table}` with BOTH present; no conversions (`!r`) or format specs (`:>10`); literal text `[A-Za-z0-9_]` only. Invalid → `LHP-CFG-063`. |
| `allowed_envs` | list[str] | None | Absent = unrestricted (any env). Empty list `[]` → `LHP-CFG-062`. `--sandbox` against a non-listed env → `LHP-CFG-065`. |

```yaml
# lhp.yaml
sandbox:
  table_pattern: "{namespace}__{table}"
  allowed_envs: [dev]
```

Block not a mapping or any non-`table_pattern` field invalid → `LHP-CFG-062`.

## What gets renamed vs what does not

The rename set = every table PRODUCED by the in-scope pipelines: streaming-table
/ materialized-view `write_target` destinations plus delta-sink
`options.tableName`. Matching is canonical (case-insensitive, backtick-stripped);
the rewrite formats the original leaf spelling, so the author's casing survives.

| Renamed (table leaf via `table_pattern`) | NOT touched |
|------------------------------------------|-------------|
| ST/MV `write_target` table (the write itself) | Reads of out-of-scope / shared tables (stay shared) |
| Delta-sink `write_target.options.tableName` (only `sink_type: delta`) | Non-delta sinks and non-delta loads (no table identity) |
| Snapshot-CDC `snapshot_cdc_config.source` (dotted-ref form) | Snapshot-CDC `source_function` (a file, not a table) |
| Delta-load `source.{catalog,schema,table}` of an in-scope table | `depends_on` entries (DAG-only, never in generated code) |
| `action.source` string / list entries matching the rename set | Bare 1-part view names (rename set holds only 2-/3-part keys) |
| Test `reference` / `lookup_table` dotted refs | Per-pipeline explicit `event_log:` dicts |
| SQL bodies in generated `spark.sql(...)` literals (table leaf only; refs inside SQL quoted strings and `--` / `/* */` comments are exempt) | Refs inside SQL string literals and comments |
| Python table literals in copied modules (custom datasource / transform files): direct string-literal args and `spark.sql(...)` constant bodies | Indirect Python reads (variable, f-string, concat, `.format`) — left untouched, flagged `LHP-VAL-066` |
| YAML parameter values bound into user Python code — python-transform `parameters`, python-load `source.parameters`, snapshot-CDC `source_function.parameters` (whole-value canonical match only; nested lists/dicts walked element-wise) | Arbitrary parameter strings that don't canonically match the rename set |

**Canonical example.** Profile: namespace `alice`, pipelines `bronze_*` +
`silver_orders`; default pattern. `edw_bronze.customer` is produced by a
`bronze_*` pipeline → it becomes `edw_bronze.alice_customer` on the write and in
every in-scope read (YAML fields, SQL, Python literals). A read of
`edw_ref.country` — produced outside the scope — stays `edw_ref.country`.

## Run behavior

- **Scoped worklist:** only profile-matched pipelines generate; `generated/<env>/`
  and `resources/lhp/` end up containing exactly them.
- **Event log:** the project-level event-log table name is namespaced through the
  same `table_pattern`; per-pipeline explicit `event_log:` dicts are NOT rewritten
  (v1 limitation).
- **Monitoring:** the monitoring phase is skipped entirely (no monitoring
  artifacts written). The monitoring pipeline is silently excluded from glob
  expansion; naming it with an exact profile entry → `LHP-VAL-064`.
- **Mixed producers:** an in-scope rename target also produced by an out-of-scope
  pipeline is still renamed; all such findings fold into one `LHP-VAL-065` warning
  naming the out-of-scope producers.
- A rewritten Python module that fails to re-parse fails the flowgroup cleanly
  (all-or-nothing run) — never a corrupt file.

## Error / warning codes

| Code | Kind | Meaning |
|------|------|---------|
| **LHP-IO-025** | error | `.lhp/profile.yaml` missing at the project root |
| **LHP-CFG-062** | error | Invalid `sandbox:` block in `lhp.yaml` (non-mapping, unknown `strategy`, empty `allowed_envs`) |
| **LHP-CFG-063** | error | Invalid `table_pattern` (placeholders/conversions/literal text) |
| **LHP-CFG-064** | error | Invalid `.lhp/profile.yaml` (YAML, root shape, missing `sandbox:` key, `namespace` regex, empty `pipelines`) |
| **LHP-CFG-065** | error | Env not in `sandbox.allowed_envs` |
| **LHP-VAL-064** | error | Profile entry matched zero pipelines, or an exact entry names the monitoring pipeline |
| **LHP-VAL-065** | **warning** | Mixed-producer sink table (also produced out of scope); rename proceeds |
| **LHP-VAL-066** | **warning** | Unrewritable in-scope Python read (non-literal argument); source untouched. Generate-only in v1 |

Warnings carry category `sandbox` and never fail a run — unless `--strict`
promotes warnings to failures.

## v1 limitations

- `table` strategy only — no schema/catalog rename strategies yet.
- No teardown command: `databricks bundle destroy` removes sandbox streaming
  tables and materialized views, but delta-sink tables created by sandbox runs
  need manual cleanup.
- `lhp validate --sandbox` applies the structured renames but emits no
  `LHP-VAL-066` (generate-only).
- Per-pipeline explicit `event_log:` dicts are not rewritten — only the
  project-level composed event-log table name is namespaced.
