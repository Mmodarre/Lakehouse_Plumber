# Error Reference

When LHP encounters a problem, it displays a structured error with code `LHP-{CATEGORY}-{NUMBER}`.

Terminal output includes: error code, description, context, fix suggestions, and example.

**Tip:** Use `--verbose` with any LHP command for additional debug information.

---

## Error Categories

| Category | Prefix | Description |
|----------|--------|-------------|
| Configuration | `CFG` | Invalid/conflicting settings in YAML, presets, templates, or bundle config |
| Validation | `VAL` | Missing required fields, invalid values, structural problems in actions |
| I/O | `IO` | Files not found, read/write failures, format issues |
| Action | `ACT` | Unknown action types, subtypes, or preset names |
| Dependency | `DEP` | Circular dependencies between views or preset inheritance |

---

## Configuration Errors (LHP-CFG)

| Code | Trigger | Fix |
|------|---------|-----|
| **CFG-001** | Same option in multiple places (e.g., `format` + `cloudFiles.format`) | Remove the legacy field, keep only `options:` format |
| **CFG-006** | `event_log` is not a YAML mapping | Define as mapping: `event_log: {catalog: ..., schema: ...}` |
| **CFG-007** | `event_log` missing `catalog` or `schema` | Add both required fields, or `enabled: false` |
| **CFG-008** | Invalid `monitoring` config (not a mapping, missing event_log, bad MVs) | See [monitoring.md](monitoring.md) troubleshooting table |
| **CFG-009** | YAML parsing error (bad indent, unquoted special chars) | Quote strings with `:` `{` `}` `[` `]`; use YAML linter |
| **CFG-010** | Deprecated field name | Replace with the field name shown in error message |
| **CFG-012** | Missing required template parameters | Add `template_parameters:` with all required params |
| **CFG-020** | Bundle resource generation error | Check `resources/lhp/` for valid YAML; run `--force` to regenerate |
| **CFG-021** | Bundle YAML processing error | Validate `databricks.yml` syntax; check UTF-8 encoding |
| **CFG-022** | Missing `databricks.yml` | Run `lhp init` or use `--no-bundle` |
| **CFG-023** | Substitution env without matching bundle target | Add missing target to `databricks.yml` |
| **CFG-024** | Bundle template fetch error | Check network; verify template path/URL |
| **CFG-025** | Bundle configuration structural error | Review `databricks.yml` against DAB docs |
| **CFG-027** | Template not found | Check spelling; run `lhp list_templates` |

## Validation Errors (LHP-VAL)

| Code | Trigger | Fix |
|------|---------|-----|
| **VAL-001** | Missing required field (source, target, type) | Add the field shown in error; check action type requirements |
| **VAL-002** | Multiple validation issues in one action | Fix each `✗` item in the error list |
| **VAL-006** | Invalid field value (typo, wrong context) | Check spelling; use valid values for the field |
| **VAL-007** | Invalid `readMode` | Use `stream` or `batch` only |
| **VAL-008** | Wrong data type (string where dict expected) | Check YAML structure matches expected format |
| **VAL-010** | Both `__eventlog_monitoring` alias and real pipeline name in config | Use only one — alias or real name |
| **VAL-011** | `__eventlog_monitoring` in a pipeline list | Must be standalone `pipeline:` entry in separate document |
| **VAL-011** | Schema syntax error (bad column type) | Fix type names; valid: STRING, BIGINT, INT, DECIMAL, etc. |
| **VAL-012** | Invalid source format (string where dict needed) | Provide full source config with `type`, `path`, etc. |

## I/O Errors (LHP-IO)

| Code | Trigger | Fix |
|------|---------|-----|
| **IO-001** | Referenced file not found | Check path spelling; paths are relative to YAML file location |
| **IO-003** | Wrong document count (empty file or unexpected `---`) | Schema/expectations files must have exactly 1 document; `---` separators only for flowgroup files |

## Action Errors (LHP-ACT)

| Code | Trigger | Fix |
|------|---------|-----|
| **ACT-001** | Unknown type, subtype, sink type, or preset name | Check spelling; error includes "Did you mean?" and valid values |

## Dependency Errors (LHP-DEP)

| Code | Trigger | Fix |
|------|---------|-----|
| **DEP-001** | Circular dependency (A → B → C → A) | Break the cycle; error shows full path. Use `lhp deps --format dot` to visualize |

---

## Common Before/After Fixes

### CFG-001: Configuration Conflict

```yaml
# BEFORE (error)
source:
  type: cloudfiles
  path: /data/events/
  format: json              # Legacy field
  options:
    cloudFiles.format: json  # Same setting — conflict!

# AFTER (fixed)
source:
  type: cloudfiles
  path: /data/events/
  options:
    cloudFiles.format: json  # Use only options format
```

### VAL-001: Missing Source

```yaml
# BEFORE (error)
actions:
  - name: load_customers
    type: load
    target: v_raw_customers
    # Missing: source!

# AFTER (fixed)
actions:
  - name: load_customers
    type: load
    source:
      type: cloudfiles
      path: /data/customers/
      options:
        cloudFiles.format: csv
    target: v_raw_customers
```

### CFG-009: YAML Parsing / Quoting Braces

```yaml
# BEFORE (error)
source:
  path: /data/{date}/events    # Braces need quoting
  comment: Load events: raw    # Colon needs quoting

# AFTER (fixed)
source:
  path: "/data/{date}/events"
  comment: "Load events: raw"
```

### IO-001: File Not Found

```yaml
# BEFORE (error)
sql_file: sqls/transform.sql   # Wrong directory name

# AFTER (fixed)
sql_file: sql/transform.sql    # Correct path
```

---

## Diagnostic Commands

```bash
lhp validate --env <env>                  # Validate all configurations
lhp validate --env <env> --verbose        # Verbose — extra debug info
lhp list_templates                        # List available templates
lhp list_presets                          # List available presets
lhp deps --format dot --env <env>         # Visualize dependencies (spot cycles)
lhp show <flowgroup> --env <env>          # Show resolved config
lhp substitutions --env <env>             # List substitution tokens for env
lhp state --env <env>                     # Inspect state file (checksums, stale)
```

---

## Symptom-Based Troubleshooting

Task-shaped entries — start from what the user sees, find the fix. Use these
before grepping the catalog above.

### `lhp generate` aborts with an error banner

Read the error code prefix:

- `LHP-CFG-*` — fix YAML/preset/template/bundle config
- `LHP-VAL-*` — fix missing/invalid fields in actions
- `LHP-IO-*` — fix file path (paths are relative to FlowGroup YAML)
- `LHP-ACT-*` — fix typo in action type/sub_type/preset name
- `LHP-DEP-*` — break the dependency cycle shown in the message

Apply the numbered fix suggestions in the terminal output, then re-run.

### `lhp validate` lists multiple errors for one action

Fix the **first** error first — later errors often cascade. For `LHP-VAL-002`,
each `✗` marker is a separate issue. Use `lhp show <flowgroup> --env <env>` to
see the resolved config (after preset merge + template expansion).

### Pipeline deploys but does not run / runs stale code

1. Run `lhp generate --env <env>` before `databricks bundle deploy --target <env>`.
2. `--env` and `--target` must match.
3. Check `resources/lhp/` contains the pipeline resource file.
4. Force regen: `lhp generate --env <env> --force`.
5. If missing `databricks.yml` (`LHP-CFG-022`): run `lhp init` or use `--no-bundle`.
6. If env has no matching bundle target (`LHP-CFG-023`): add target to `databricks.yml`.

### YAML completion / IntelliSense not working

JSON Schemas live under `src/lhp/schemas/` in the installed package. The editor
must map them to `pipelines/*.yaml`, `presets/*.yaml`, `templates/*.yaml`,
`substitutions/*.yaml`. Reload editor window after LHP install/upgrade.

### Substitutions not resolved (literal `${token}` in output)

Substitution order: `%{local_var}` → `{{ template_param }}` → `${env_token}` →
`${secret:scope/key}`. Bare-braces `{token}` is **deprecated**.

1. `lhp substitutions --env <env>` — list known tokens.
2. Add missing token to `substitutions/<env>.yaml`.
3. Token must appear inside a string value, not as a YAML key.
4. `lhp show <flowgroup> --env <env>` — inspect resolved config; unresolved
   tokens appear unchanged.

### Preset/template/blueprint edits not picked up

LHP regenerates FlowGroups by content checksum. Most edits trigger regen on
next `lhp generate`. If state is out of sync:

```bash
lhp generate --env <env> --force      # Force regen, keep state
rm .lhp_state.json                    # Nuclear: drop state, regen all
lhp generate --env <env>
```

Confirm the preset/template is actually referenced via `lhp show <flowgroup>`.

### `lhp generate` says nothing to do after editing

LHP does not checksum every indirectly-referenced file (e.g., external `.sql`
loaded via `sql_file`). Use `--force` after editing such files.

```bash
lhp generate --env <env> --dry-run --verbose   # Show what would happen + why
lhp state --env <env>                          # Tracked files, checksums, stale
lhp generate --env <env> --force               # Bypass state check
```

### CLI flags reference

Only these flags exist on `lhp generate`:

- `--env <name>` — required
- `--force` — bypass state check, regenerate all
- `--dry-run` — preview without writing
- `--no-bundle` — skip bundle resource generation
- `--include-tests` — include test actions in output
- `--pipeline <name>` — target a single pipeline
- `--verbose` / `-v` — extra logging

Do **not** use `--force-all`, `--show-dependencies`, or `--check-cycles` — those
flags do not exist (despite appearing in some older docs).

### POSIX exit codes (from `src/lhp/utils/exit_codes.py`)

| Code | Meaning | LHP categories |
|------|---------|----------------|
| 0 | Success | — |
| 1 | General error | unknown |
| 64 | Usage error | bad CLI args |
| 65 | Data error | VAL, DEP, ACT |
| 66 | No input | IO |
| 70 | Software error | internal bug |
| 74 | I/O error | permissions, disk |
| 78 | Config error | CFG, CloudFiles |

Script integrations should branch on exit code, not on stderr text.
