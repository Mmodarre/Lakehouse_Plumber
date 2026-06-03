# Blueprints Reference

A **blueprint** is a parameterised collection of FlowGroup specs, expanded once per *instance* file. Each `(blueprint, instance, spec)` triple yields one synthetic FlowGroup that joins the normal generation pipeline alongside hand-written FlowGroups.

## When to use a blueprint (vs template vs preset)

| Artifact | Granularity | Reuses | Reference syntax | Param syntax |
|----------|-------------|--------|------------------|--------------|
| **Preset** | Action defaults | Config defaults by action type | `presets: [name]` | — |
| **Template** | Actions **within one** FlowGroup | One parameterised FlowGroup | `use_template:` + `template_parameters:` | Jinja2 `{{ var }}` |
| **Blueprint** | **Whole FlowGroups, many instances** | The same FlowGroup shape across N deployments | `use_blueprint:` + `parameters:` | `%{var}` |

Reach for a blueprint when **the same FlowGroup(s) repeat across sites / regions / tenants** and differ only by a few values (one instance file per variant). Use a template when you parameterise the actions inside a *single* flowgroup. When unsure, write concrete flowgroups first and extract a blueprint only once the cross-deployment repetition is real.

Math: **N specs × M instances = N×M** synthetic FlowGroups.

## Blueprint file

Lives under `blueprint_include` (default `blueprints/**/*.{yaml,yml}`). Single-document YAML matching the `Blueprint` model.

```yaml
# blueprints/erp_ingestion.yaml
name: erp_ingestion
version: "1.0"
description: "Standard ERP ingestion for all regional sites"

parameters:
  - name: site_name
    required: true
    description: "Site identifier"
  - name: partition_key
    required: false
    default: order_date

flowgroups:                                   # one or more FlowGroup specs
  - pipeline: "%{site_name}_erp_raw"          # %{var} only — no ${...} here
    flowgroup: "%{site_name}_orders_ingestion"
    actions:
      - name: load_orders
        type: load
        source:
          type: cloudfiles
          path: "/Volumes/raw/erp/%{site_name}/orders"
          format: json
        target: v_orders_raw
      - name: write_orders
        type: write
        source: v_orders_raw
        write_target:
          type: streaming_table
          catalog: "${catalog}"
          schema: bronze
          table: "%{site_name}_orders_raw"
          partition_columns: ["%{partition_key}"]
```

| Field | Type | Required | Notes |
|-------|------|----------|-------|
| `name` | string | yes | Referenced by instances via `use_blueprint`. |
| `version` | string | no | Defaults `"1.0"`. |
| `description` | string | no | — |
| `parameters` | list | no | Each: `name` (req), `required` (default `false`), `default`, `description`. |
| `flowgroups` | list | **yes** | Non-empty. Each spec mirrors a flowgroup: `pipeline`, `flowgroup`, plus `use_template`/`template_parameters`, `actions`, `presets`, `variables`, `operational_metadata`, `job_name`. A spec's `variables` win over instance `parameters` on name conflict. |

A file is detected as a blueprint by **shape**: it has `parameters` + `flowgroups` and **no** `actions` at top level (not by directory).

## Instance file

Lives under `instance_include` (default `pipelines/**/*.{yaml,yml}`) — alongside hand-written flowgroups. Detected as an instance by the presence of `use_blueprint:` (or legacy `blueprint:`).

```yaml
# pipelines/erp/bronze/erp_acme_BP_erp_ingestion.yaml
use_blueprint: erp_ingestion          # matches blueprint `name`
parameters:
  site_name: acme
  partition_key: order_date
```

- **Preferred syntax**: `use_blueprint: <name>` + nested `parameters:`.
- **Legacy syntax** (`blueprint: <name>` + flat top-level keys) is **deprecated**. Mixing the two forms is a **hard error `LHP-VAL-061`**, not a warning. Always emit the `use_blueprint:` form.
- Naming convention (BP-3.11): `<system>_<layer>_<variant>_<BPxxx>.yaml`.

## Parameter resolution

- `%{var}` is the blueprint parameter/variable syntax. It resolves in `pipeline:` / `flowgroup:` fields **first** (before expansion), and everywhere else **after** expansion.
- **`${env_token}` and `${secret:scope/key}` are REJECTED in `pipeline:` / `flowgroup:` fields → `LHP-VAL-044`** (those fields form the source-path index before env substitution runs). Use `${...}` freely in any other field (catalog, schema, paths, options).
- An unresolved `%{var}` in `pipeline:` / `flowgroup:` → `LHP-VAL-055`.
- Effective parameter map (last wins): blueprint `default`s → instance `parameters` → spec `variables`.

## CLI

```bash
lhp list blueprints                 # list discovered blueprints (param + instance counts)
lhp list blueprints --instances     # also show each instance and the pipelines it resolves to
lhp dag --expand-blueprints         # one node per instance (default dedupes by blueprint spec)
lhp dag --blueprint erp_ingestion   # restrict dependency analysis to one blueprint
lhp validate --env dev              # discovers, expands, and validates synthetic flowgroups
lhp generate --env dev              # expands blueprints into generated code
```

> There is **no `lhp show --instance`** on this branch — older docs reference it; it has been removed.

## Error codes (blueprint family)

| Code | Meaning |
|------|---------|
| `LHP-CFG-040` | A file under the flowgroup glob is blueprint-shaped (has `parameters`+`flowgroups`) but in the wrong place. |
| `LHP-VAL-041`…`046` | Blueprint/instance structural validation (bad references, parameter mismatches). |
| `LHP-VAL-044` | `${...}` used in `pipeline:`/`flowgroup:` field (not allowed there). |
| `LHP-VAL-053` / `055` | Parameter resolution failures (unresolved `%{var}`, incl. in pipeline/flowgroup fields). |
| `LHP-VAL-061` | Legacy `blueprint:` and `use_blueprint:` forms mixed — hard error. |
| `LHP-CFG-054` | Malformed instance: `use_blueprint:`/`blueprint:` is not a single non-empty string. |

When troubleshooting, run `lhp validate --env <env> --verbose` and consult [errors.md](errors.md) for the full registry.
