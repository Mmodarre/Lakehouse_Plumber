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
```
