/**
 * Per-section validators for lhp.yaml (internal to the config-model layer;
 * the public entry point is `projectConfig.ts`).
 *
 * Each function mirrors one `src/lhp/core/loaders/_*_config_parser.py`
 * module; errors are emitted only where that parser raises. All issues use
 * docIndex 0 — lhp.yaml is a single-document file.
 */
import { tablePatternViolation } from './_pythonFormat'
import type { ValidationIssue } from './validators'
import {
  errorIssue,
  intRangeCheck,
  isPlainObject,
  isStringArray,
  parseLaxBool,
  warningIssue,
} from './validators'

/**
 * Message for the loader's silent-discard path: a top-level Pydantic
 * rejection is SWALLOWED by load_project_config (the `except ValueError`
 * at project_config_loader.py:72-87 falls through without re-raising) and
 * the CLI runs on with NO project config at all — hence warning severity.
 */
export function swallowedTypeMessage(field: string, expected: string): string {
  return `'${field}' must be ${expected}; the loader logs this and then IGNORES lhp.yaml entirely (project treated as unconfigured)`
}

/** include/blueprint_include/instance_include (loaders/_include_patterns_parser.py:37-76). */
export function validateIncludeList(
  value: unknown,
  key: string,
  issues: ValidationIssue[],
): void {
  if (!Array.isArray(value)) {
    issues.push(errorIssue(0, [key], `'${key}' must be a list of glob patterns`, 'CFG_003'))
    return
  }
  value.forEach((pattern, index) => {
    if (typeof pattern !== 'string') {
      issues.push(
        errorIssue(0, [key, index], `Pattern at index ${index} must be a string`, 'CFG_004'),
      )
      return
    }
    // Glob validity mirrors utils/file_pattern_matcher.py:40-58: empty
    // patterns and the two malformed markers are rejected;
    // fnmatch.translate accepts everything else.
    if (!pattern || pattern.includes('***/') || pattern.includes('[unclosed')) {
      issues.push(
        errorIssue(0, [key, index], `'${pattern}' is not a valid glob pattern`, 'CFG_005'),
      )
    }
  })
}

/** operational_metadata (loaders/_operational_metadata_config_parser.py). */
export function validateOperationalMetadata(value: unknown, issues: ValidationIssue[]): void {
  const base = ['operational_metadata']
  if (!isPlainObject(value)) {
    // A non-mapping crashes the parser (uncaught -> CFG_002).
    issues.push(errorIssue(0, base, `'operational_metadata' must be a mapping`, 'CFG_002'))
    return
  }

  const definedColumns = new Set<string>()
  if ('columns' in value) {
    const columns = value.columns
    if (!isPlainObject(columns)) {
      issues.push(errorIssue(0, [...base, 'columns'], `'columns' must be a mapping`, 'CFG_002'))
    } else {
      for (const [name, columnConfig] of Object.entries(columns)) {
        definedColumns.add(name)
        const path = [...base, 'columns', name]
        if (typeof columnConfig === 'string') continue // bare-string shorthand (:24-26)
        if (!isPlainObject(columnConfig)) {
          issues.push(
            errorIssue(0, path, `Column '${name}' must be a mapping or a bare expression string`, 'CFG_003'),
          )
          continue
        }
        if ('expression' in columnConfig && typeof columnConfig.expression !== 'string') {
          issues.push(errorIssue(0, [...path, 'expression'], `'expression' must be a string`, 'CFG_003'))
        }
        if ('applies_to' in columnConfig && !isStringArray(columnConfig.applies_to)) {
          issues.push(
            errorIssue(0, [...path, 'applies_to'], `'applies_to' must be a list of target types`, 'CFG_003'),
          )
        }
        if (
          'additional_imports' in columnConfig &&
          columnConfig.additional_imports !== null &&
          !isStringArray(columnConfig.additional_imports)
        ) {
          issues.push(
            errorIssue(0, [...path, 'additional_imports'], `'additional_imports' must be a list of strings`, 'CFG_003'),
          )
        }
      }
    }
  }

  const presetColumns = new Map<string, string[]>()
  if ('presets' in value) {
    const presets = value.presets
    if (!isPlainObject(presets)) {
      issues.push(errorIssue(0, [...base, 'presets'], `'presets' must be a mapping`, 'CFG_002'))
    } else {
      for (const [name, presetConfig] of Object.entries(presets)) {
        const path = [...base, 'presets', name]
        let columnList: unknown
        if (Array.isArray(presetConfig)) {
          columnList = presetConfig // bare-list shorthand (:62-64)
        } else if (isPlainObject(presetConfig)) {
          columnList = 'columns' in presetConfig ? presetConfig.columns : []
        } else {
          issues.push(
            errorIssue(0, path, `Preset '${name}' must be a mapping or a bare column list`, 'CFG_004'),
          )
          continue
        }
        if (!isStringArray(columnList)) {
          issues.push(
            errorIssue(0, path, `Preset '${name}' columns must be a list of column names`, 'CFG_004'),
          )
          continue
        }
        presetColumns.set(name, columnList)
      }
    }
  }

  // Every referenced column must be defined (:104-125).
  for (const [presetName, columnList] of presetColumns) {
    for (const columnName of columnList) {
      if (!definedColumns.has(columnName)) {
        issues.push(
          errorIssue(
            0,
            [...base, 'presets', presetName],
            `Preset '${presetName}' references undefined column '${columnName}'`,
            'CFG_005',
          ),
        )
      }
    }
  }

  if ('defaults' in value && Boolean(value.defaults) && !isPlainObject(value.defaults)) {
    // Rejected by Pydantic AFTER the parser's own try/excepts, so it hits
    // the same swallow path as the top-level scalars — warning, not error.
    issues.push(
      warningIssue(0, [...base, 'defaults'], swallowedTypeMessage('operational_metadata.defaults', 'a mapping')),
    )
  }
}

/**
 * event_log (loaders/_event_log_config_parser.py). Returns `{enabled}` when
 * the section is well-formed enough for the loader to have constructed it
 * (monitoring's cross-check needs that), or null when the loader raises.
 */
export function validateEventLog(
  value: unknown,
  issues: ValidationIssue[],
): { enabled: boolean } | null {
  const base = ['event_log']
  if (!isPlainObject(value)) {
    // Unlike monitoring, a bare `event_log:` (null) is NOT treated as {} —
    // the parser requires a mapping (:14-23).
    issues.push(errorIssue(0, base, `'event_log' must be a mapping`, 'CFG_006'))
    return null
  }

  let typeError = false
  for (const field of ['catalog', 'schema']) {
    if (field in value && value[field] !== null && typeof value[field] !== 'string') {
      issues.push(errorIssue(0, [...base, field], `'${field}' must be a string`, 'CFG_006'))
      typeError = true
    }
  }
  for (const field of ['name_prefix', 'name_suffix']) {
    if (field in value && typeof value[field] !== 'string') {
      issues.push(errorIssue(0, [...base, field], `'${field}' must be a string`, 'CFG_006'))
      typeError = true
    }
  }

  let enabled = true // default (:27)
  if ('enabled' in value) {
    const parsed = parseLaxBool(value.enabled)
    if (parsed === undefined) {
      issues.push(errorIssue(0, [...base, 'enabled'], `'enabled' must be a boolean`, 'CFG_006'))
      return null
    }
    enabled = parsed
  }

  // When enabled, catalog AND schema are required and non-empty (:48-71) —
  // skipped when a type error above means the loader raised before this.
  if (enabled && !typeError) {
    const missing = ['catalog', 'schema'].filter((field) => {
      const fieldValue = value[field]
      return fieldValue === undefined || fieldValue === null || fieldValue === ''
    })
    if (missing.length > 0) {
      issues.push(
        errorIssue(
          0,
          base,
          `event_log is enabled but missing required fields: ${missing.join(', ')}. Both 'catalog' and 'schema' are required when event_log is enabled`,
          'CFG_007',
        ),
      )
    }
  }

  return typeError ? null : { enabled }
}

/** monitoring (loaders/_monitoring_config_parser.py). */
export function validateMonitoring(
  value: unknown,
  eventLog: { present: boolean; state: { enabled: boolean } | null },
  issues: ValidationIssue[],
): void {
  const base = ['monitoring']
  // A bare `monitoring:` (null) means all defaults — enabled! (:26-27)
  const data = value === null || value === undefined ? {} : value
  if (!isPlainObject(data)) {
    issues.push(errorIssue(0, base, `'monitoring' must be a mapping`, 'CFG_008'))
    return
  }

  // Parse-time checks — these run EVEN when monitoring is disabled.
  // materialized_views shape (:42-72):
  let materializedViews: Record<string, unknown>[] | null = null
  let mvShapeError = false
  const rawViews = data.materialized_views
  if (rawViews !== undefined && rawViews !== null) {
    if (!Array.isArray(rawViews)) {
      issues.push(
        errorIssue(0, [...base, 'materialized_views'], `'materialized_views' must be a list of view definitions`, 'CFG_008'),
      )
      mvShapeError = true
    } else {
      materializedViews = []
      rawViews.forEach((view, index) => {
        if (!isPlainObject(view)) {
          issues.push(
            errorIssue(0, [...base, 'materialized_views', index], `Each materialized view must be a mapping`, 'CFG_008'),
          )
          mvShapeError = true
        } else {
          materializedViews?.push(view)
        }
      })
    }
  }

  // max_concurrent_streams: integer (bool rejected) in 1..20 (:74-101).
  const maxStreams = 'max_concurrent_streams' in data ? data.max_concurrent_streams : 10
  if (intRangeCheck(maxStreams, 1, 20) !== 'ok') {
    issues.push(
      errorIssue(
        0,
        [...base, 'max_concurrent_streams'],
        `'max_concurrent_streams' must be an integer in the range 1..20`,
        'CFG_008',
      ),
    )
  }

  // Pydantic field types (models/_monitoring.py:42-55).
  const brokenFields = new Set<string>()
  for (const field of ['pipeline_name', 'catalog', 'schema', 'job_config_path']) {
    if (field in data && data[field] !== null && typeof data[field] !== 'string') {
      issues.push(errorIssue(0, [...base, field], `'${field}' must be a string`, 'CFG_008'))
      brokenFields.add(field)
    }
  }
  for (const field of ['streaming_table', 'checkpoint_path']) {
    if (field in data && typeof data[field] !== 'string') {
      issues.push(errorIssue(0, [...base, field], `'${field}' must be a string`, 'CFG_008'))
      brokenFields.add(field)
    }
  }
  if ('enable_job_monitoring' in data && parseLaxBool(data.enable_job_monitoring) === undefined) {
    issues.push(
      errorIssue(0, [...base, 'enable_job_monitoring'], `'enable_job_monitoring' must be a boolean`, 'CFG_008'),
    )
  }

  let enabled = true // default (:105)
  if ('enabled' in data) {
    const parsed = parseLaxBool(data.enabled)
    if (parsed === undefined) {
      issues.push(errorIssue(0, [...base, 'enabled'], `'enabled' must be a boolean`, 'CFG_008'))
      return
    }
    enabled = parsed
  }

  // Everything below only applies when enabled (:139-140).
  if (!enabled) return

  // Requires a present AND enabled event_log (:142-155). When the event_log
  // section itself is broken the loader raised there first — skip the
  // cross-check to avoid cascading noise.
  if (!eventLog.present) {
    issues.push(
      errorIssue(0, base, `monitoring is enabled but event_log is missing; the monitoring pipeline needs event_log tables`, 'CFG_008'),
    )
  } else if (eventLog.state !== null && !eventLog.state.enabled) {
    issues.push(
      errorIssue(0, base, `monitoring is enabled but event_log is disabled; the monitoring pipeline needs event_log tables`, 'CFG_008'),
    )
  }

  // checkpoint_path and job_config_path are required non-empty (:157-187);
  // skipped per-field when a type error above means the loader never got
  // here. The job_config_path FILE-EXISTENCE check (:193-208) is not
  // mirrored — it needs the filesystem.
  for (const field of ['checkpoint_path', 'job_config_path']) {
    if (brokenFields.has(field)) continue
    const fieldValue = data[field]
    if (fieldValue === undefined || fieldValue === null || fieldValue === '') {
      issues.push(
        errorIssue(0, [...base, field], `'monitoring.${field}' must be set when monitoring is enabled`, 'CFG_008'),
      )
    }
  }

  // Materialized-view content rules (:210-249): name required, names unique,
  // and AT MOST ONE of sql / sql_path (neither is allowed — the loader only
  // rejects both). Skipped when the list shape was broken (loader raised).
  if (materializedViews !== null && !mvShapeError) {
    const seenNames = new Set<unknown>()
    materializedViews.forEach((view, index) => {
      const path = [...base, 'materialized_views', index]
      const name = view.name
      if (name === undefined || name === null || name === '') {
        issues.push(errorIssue(0, path, `Materialized view at index ${index} has no 'name'`, 'CFG_008'))
        return
      }
      if (seenNames.has(name)) {
        issues.push(
          errorIssue(0, path, `Duplicate materialized view name '${String(name)}'`, 'CFG_008'),
        )
      }
      seenNames.add(name)
      if (Boolean(view.sql) && Boolean(view.sql_path)) {
        issues.push(
          errorIssue(0, path, `Materialized view '${String(name)}' specifies both 'sql' and 'sql_path'; only one is allowed`, 'CFG_008'),
        )
      }
    })
  }
}

/** test_reporting (loaders/_test_reporting_config_parser.py). */
export function validateTestReporting(value: unknown, issues: ValidationIssue[]): void {
  const base = ['test_reporting']
  if (!isPlainObject(value)) {
    issues.push(errorIssue(0, base, `'test_reporting' must be a mapping`, 'CFG_009'))
    return
  }
  // One error listing ALL missing required fields, like the parser (:25-41).
  const missing = ['module_path', 'function_name'].filter((field) => !(field in value))
  if (missing.length > 0) {
    issues.push(
      errorIssue(0, base, `test_reporting is missing required fields: ${missing.join(', ')}`, 'CFG_009'),
    )
  }
  for (const field of ['module_path', 'function_name']) {
    if (field in value && typeof value[field] !== 'string') {
      issues.push(errorIssue(0, [...base, field], `'${field}' must be a string`, 'CFG_009'))
    }
  }
  if ('config_file' in value && value.config_file !== null && typeof value.config_file !== 'string') {
    issues.push(errorIssue(0, [...base, 'config_file'], `'config_file' must be a string path`, 'CFG_009'))
  }
}

/** uc_tagging (loaders/_uc_tagging_config_parser.py). */
export function validateUcTagging(value: unknown, issues: ValidationIssue[]): void {
  const base = ['uc_tagging']
  // A bare `uc_tagging:` (null) opts in with all defaults (:22-24).
  if (value === null || value === undefined) return
  if (!isPlainObject(value)) {
    issues.push(errorIssue(0, base, `'uc_tagging' must be a mapping`, 'CFG_009'))
    return
  }
  // STRICT booleans — the parser uses isinstance(x, bool) (:37-49), so
  // "yes"/1 are rejected here even though Pydantic elsewhere accepts them.
  for (const field of ['enabled', 'remove_undeclared_tags']) {
    if (field in value && typeof value[field] !== 'boolean') {
      issues.push(errorIssue(0, [...base, field], `'${field}' must be true or false`, 'CFG_009'))
    }
  }
  const concurrency = 'tag_update_concurrency' in value ? value.tag_update_concurrency : 16
  if (intRangeCheck(concurrency, 1, 20) !== 'ok') {
    issues.push(
      errorIssue(
        0,
        [...base, 'tag_update_concurrency'],
        `'tag_update_concurrency' must be a positive integer in the range 1..20`,
        'CFG_009',
      ),
    )
  }
}

/** wheel (loaders/_wheel_config_parser.py). */
export function validateWheel(value: unknown, issues: ValidationIssue[]): void {
  const base = ['wheel']
  if (!isPlainObject(value)) {
    issues.push(errorIssue(0, base, `'wheel' must be a mapping`, 'CFG_060'))
    return
  }
  if ('artifact_volume' in value && value.artifact_volume !== null && typeof value.artifact_volume !== 'string') {
    issues.push(
      errorIssue(0, [...base, 'artifact_volume'], `'artifact_volume' must be a string path`, 'CFG_060'),
    )
  }
}

/** sandbox (loaders/_sandbox_config_parser.py + models/_sandbox.py). */
export function validateSandbox(value: unknown, issues: ValidationIssue[]): void {
  const base = ['sandbox']
  if (!isPlainObject(value)) {
    issues.push(errorIssue(0, base, `'sandbox' must be a mapping`, 'CFG_062'))
    return
  }
  // strategy is Literal["table"] (models/_sandbox.py:29).
  if ('strategy' in value && value.strategy !== 'table') {
    issues.push(
      errorIssue(0, [...base, 'strategy'], `'strategy' must be 'table' (the only supported strategy)`, 'CFG_062'),
    )
  }
  if ('table_pattern' in value) {
    const pattern = value.table_pattern
    if (typeof pattern !== 'string') {
      issues.push(errorIssue(0, [...base, 'table_pattern'], `'table_pattern' must be a string`, 'CFG_062'))
    } else {
      const violation = tablePatternViolation(pattern)
      if (violation !== null) {
        issues.push(
          errorIssue(0, [...base, 'table_pattern'], `table_pattern '${pattern}' is invalid: ${violation}`, 'CFG_063'),
        )
      }
    }
  }
  if ('allowed_envs' in value && value.allowed_envs !== null) {
    const environments = value.allowed_envs
    if (!isStringArray(environments)) {
      issues.push(
        errorIssue(0, [...base, 'allowed_envs'], `'allowed_envs' must be a list of environment names, or omitted`, 'CFG_062'),
      )
    } else if (environments.length === 0) {
      // Empty list forbids sandbox mode everywhere (parser :70-85).
      issues.push(
        errorIssue(0, [...base, 'allowed_envs'], `'allowed_envs' must not be an empty list; omit it to allow all environments`, 'CFG_062'),
      )
    }
  }
}
