/**
 * View-model + pure client-side validation for lhp.yaml (project config).
 *
 * Ground truth: `src/lhp/core/loaders/project_config_loader.py`, the
 * per-section parsers in `src/lhp/core/loaders/_*_config_parser.py`
 * (mirrored in `./_projectSections.ts`), and the Pydantic models in
 * `src/lhp/models/`. Errors are emitted only where the Python side
 * hard-fails with a typed CFG_* raise; loader-permissive spots stay
 * permissive. Two loader behaviors shape the severities here:
 *
 * - `name` is NOT required — the loader falls back to "unnamed_project"
 *   (project_config_loader.py:151), so a missing name is a warning.
 * - Top-level Pydantic type rejections (name/version/… wrong type) are
 *   SWALLOWED by load_project_config (`except ValueError` at :72-87 falls
 *   through without re-raising, returning None): the CLI runs on with NO
 *   project config. Runtime-verified — warnings, not errors.
 */
import {
  swallowedTypeMessage,
  validateEventLog,
  validateIncludeList,
  validateMonitoring,
  validateOperationalMetadata,
  validateSandbox,
  validateTestReporting,
  validateUcTagging,
  validateWheel,
} from './_projectSections'
import type { ValidationIssue } from './validators'
import { errorIssue, isPlainObject, parseLaxBool, warningIssue } from './validators'

/** Top-level keys the loader reads (project_config_loader.py:105-168). */
export const PROJECT_KNOWN_KEYS: ReadonlySet<string> = new Set([
  'name',
  'version',
  'description',
  'author',
  'created_date',
  'include',
  'blueprint_include',
  'instance_include',
  'operational_metadata',
  'event_log',
  'monitoring',
  'required_lhp_version',
  'test_reporting',
  'uc_tagging',
  'wheel',
  'sandbox',
  'apply_formatting',
])

/** Optional feature sections, in canonical display order. */
export const PROJECT_OPTIONAL_SECTIONS = [
  'operational_metadata',
  'event_log',
  'monitoring',
  'uc_tagging',
  'test_reporting',
  'wheel',
  'sandbox',
] as const

// --- View-model types (ACCEPTED INPUT shapes, shorthands included) --------

/** Column object form (models/_operational_metadata.py:8-13). */
export interface MetadataColumnSpec {
  expression?: string
  description?: string
  applies_to?: string[]
  additional_imports?: string[]
  enabled?: boolean
}

/** A column value: bare string = the expression (parser shorthand :24-26). */
export type MetadataColumnInput = string | MetadataColumnSpec

/** Preset object form (models/_operational_metadata.py:16-18). */
export interface MetadataPresetSpec {
  columns?: string[]
  description?: string
}

/** A preset value: bare list = the columns list (parser shorthand :62-64). */
export type MetadataPresetInput = string[] | MetadataPresetSpec

export interface OperationalMetadataSection {
  columns?: Record<string, MetadataColumnInput>
  presets?: Record<string, MetadataPresetInput>
  defaults?: Record<string, unknown>
}

/** event_log section (models/_monitoring.py:8-17). */
export interface EventLogSection {
  enabled?: boolean
  catalog?: string
  schema?: string
  name_prefix?: string
  name_suffix?: string
}

/** One monitoring materialized view (models/_monitoring.py:20-25). */
export interface MonitoringMaterializedView {
  name?: string
  sql?: string
  sql_path?: string
}

/** monitoring section (models/_monitoring.py:28-55). */
export interface MonitoringSection {
  enabled?: boolean
  pipeline_name?: string
  catalog?: string
  schema?: string
  streaming_table?: string
  checkpoint_path?: string
  job_config_path?: string
  max_concurrent_streams?: number
  materialized_views?: MonitoringMaterializedView[]
  enable_job_monitoring?: boolean
}

/** uc_tagging section (loaders/_uc_tagging_config_parser.py). */
export interface UcTaggingSection {
  enabled?: boolean
  remove_undeclared_tags?: boolean
  tag_update_concurrency?: number
}

/** test_reporting section (loaders/_test_reporting_config_parser.py). */
export interface TestReportingSection {
  module_path?: string
  function_name?: string
  config_file?: string
}

/** wheel section (models/_project.py:14-17). */
export interface WheelSection {
  artifact_volume?: string
}

/** sandbox section (models/_sandbox.py:26-32). */
export interface SandboxSection {
  strategy?: 'table'
  table_pattern?: string
  allowed_envs?: string[] | null
}

/** The whole lhp.yaml document; unknown keys are passthrough. */
export interface ProjectConfigModel {
  name?: string
  version?: string
  description?: string
  author?: string
  created_date?: string
  include?: string[]
  blueprint_include?: string[]
  instance_include?: string[]
  operational_metadata?: OperationalMetadataSection
  event_log?: EventLogSection
  monitoring?: MonitoringSection
  required_lhp_version?: string
  test_reporting?: TestReportingSection
  uc_tagging?: UcTaggingSection | null
  wheel?: WheelSection
  sandbox?: SandboxSection
  apply_formatting?: boolean
  [key: string]: unknown
}

// --- Presence helpers ------------------------------------------------------

/** Which optional sections are present, in canonical order. */
export function listProjectSections(doc: unknown): string[] {
  if (!isPlainObject(doc)) return []
  return PROJECT_OPTIONAL_SECTIONS.filter((section) => section in doc)
}

/** Unknown top-level keys — the loader never reads them (passthrough chips). */
export function listProjectPassthroughKeys(doc: unknown): string[] {
  if (!isPlainObject(doc)) return []
  return Object.keys(doc).filter((key) => !PROJECT_KNOWN_KEYS.has(key))
}

// --- Validation -------------------------------------------------------------

/** Validate a single lhp.yaml document (docIndex is always 0). */
export function validateProjectConfig(doc: unknown): ValidationIssue[] {
  const issues: ValidationIssue[] = []

  if (doc === null || doc === undefined) {
    // Loader warns and returns None for an empty file (:57-61).
    issues.push(warningIssue(0, [], 'Empty project configuration; the loader ignores it'))
    return issues
  }
  if (!isPlainObject(doc)) {
    issues.push(errorIssue(0, [], 'lhp.yaml must be a mapping', 'CFG_002'))
    return issues
  }

  if (!('name' in doc)) {
    issues.push(
      warningIssue(0, ['name'], `No 'name' set; the loader falls back to 'unnamed_project'`),
    )
  } else if (typeof doc.name !== 'string') {
    issues.push(warningIssue(0, ['name'], swallowedTypeMessage('name', 'a string')))
  }

  // Pydantic scalar fields (models/_project.py:24-34, 39-47) — the
  // swallow path, see module docstring.
  if ('version' in doc && typeof doc.version !== 'string') {
    issues.push(
      warningIssue(0, ['version'], swallowedTypeMessage('version', 'a string (quote it in YAML)')),
    )
  }
  for (const field of ['description', 'author', 'created_date', 'required_lhp_version']) {
    if (field in doc && doc[field] !== null && typeof doc[field] !== 'string') {
      issues.push(warningIssue(0, [field], swallowedTypeMessage(field, 'a string')))
    }
  }
  if ('apply_formatting' in doc && parseLaxBool(doc.apply_formatting) === undefined) {
    issues.push(
      warningIssue(0, ['apply_formatting'], swallowedTypeMessage('apply_formatting', 'a boolean')),
    )
  }

  for (const key of ['include', 'blueprint_include', 'instance_include']) {
    if (key in doc) validateIncludeList(doc[key], key, issues)
  }

  if ('operational_metadata' in doc) {
    validateOperationalMetadata(doc.operational_metadata, issues)
  }

  // event_log first — monitoring cross-validates against it (loader :122-130).
  let eventLog: { present: boolean; state: { enabled: boolean } | null } = {
    present: 'event_log' in doc,
    state: null,
  }
  if (eventLog.present) {
    eventLog = { present: true, state: validateEventLog(doc.event_log, issues) }
  }
  if ('monitoring' in doc) {
    validateMonitoring(doc.monitoring, eventLog, issues)
  }

  if ('test_reporting' in doc) validateTestReporting(doc.test_reporting, issues)
  if ('uc_tagging' in doc) validateUcTagging(doc.uc_tagging, issues)
  if ('wheel' in doc) validateWheel(doc.wheel, issues)
  if ('sandbox' in doc) validateSandbox(doc.sandbox, issues)

  return issues
}
