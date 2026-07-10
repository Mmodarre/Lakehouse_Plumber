/**
 * View-model + pure client-side validation for pipeline_config*.yaml.
 *
 * Ground truth is the Python loader
 * `src/lhp/core/loaders/pipeline_config_loader.py` (doc classification,
 * VAL_005/VAL_006/VAL_011 structure rules, `_validate_config` field rules
 * at :294-481) plus `src/lhp/bundle/manager.py:29-46`
 * (EXPLICITLY_RENDERED_PIPELINE_CONFIG_KEYS — everything else is rendered
 * verbatim into the bundle resource, i.e. passthrough). Errors are emitted
 * only where the loader raises; loader log-and-skip paths become warnings.
 */
import type { ValidationIssue } from './validators'
import { errorIssue, isEnumMember, isPlainObject, presentKeys, warningIssue } from './validators'

/** Built-in defaults (lowest merge layer) — pipeline_config_loader.py:18-23. */
export const PIPELINE_BUILTIN_DEFAULTS = {
  serverless: true,
  edition: 'ADVANCED',
  channel: 'CURRENT',
  continuous: false,
} as const

/** Allowed `edition` values — pipeline_config_loader.py:25. */
export const PIPELINE_ALLOWED_EDITIONS = ['CORE', 'PRO', 'ADVANCED'] as const
/** Allowed `channel` values — pipeline_config_loader.py:26. */
export const PIPELINE_ALLOWED_CHANNELS = ['CURRENT', 'PREVIEW'] as const
/** Allowed `packaging` values — pipeline_config_loader.py:27. */
export const PIPELINE_ALLOWED_PACKAGING_MODES = ['wheel', 'source'] as const
/** Standalone alias for the monitoring pipeline — pipeline_config_loader.py:28. */
export const MONITORING_PIPELINE_ALIAS = '__eventlog_monitoring'

/**
 * Keys the pipeline form renders as first-class fields: the explicitly
 * rendered bundle-resource keys (bundle/manager.py:29-46) plus `packaging`,
 * which the loader validates (:333-347) and `resolve_packaging_modes`
 * consumes. Any other key is passthrough — shown read-only, never dropped.
 */
export const PIPELINE_KNOWN_KEYS: ReadonlySet<string> = new Set([
  'catalog',
  'schema',
  'serverless',
  'clusters',
  'configuration',
  'continuous',
  'photon',
  'edition',
  'channel',
  'notifications',
  'tags',
  'event_log',
  'environment',
  'permissions',
  'packaging',
])

/** Cluster autoscale block (templates/bundle/pipeline_resource.yml.j2:34-41). */
export interface PipelineClusterAutoscale {
  min_workers?: number
  max_workers?: number
  mode?: string
}

/** One clusters[] entry (pipeline_resource.yml.j2:17-42). */
export interface PipelineCluster {
  label?: string
  node_type_id?: string
  instance_pool_id?: string
  driver_node_type_id?: string
  driver_instance_pool_id?: string
  policy_id?: string
  autoscale?: PipelineClusterAutoscale
  [key: string]: unknown
}

/** One notifications[] entry (pipeline_resource.yml.j2:72-85). */
export interface PipelineNotification {
  email_recipients?: string[]
  alerts?: string[]
}

/** Per-pipeline event_log override (object form) — pipeline_resource.yml.j2:93-99. */
export interface PipelineEventLogOverride {
  name?: string
  schema?: string
  catalog?: string
}

/** One permissions[] entry: level + exactly one principal (loader :404-481). */
export interface PipelinePermissionEntry {
  level?: string
  user_name?: string
  group_name?: string
  service_principal_name?: string
  [key: string]: unknown
}

/**
 * The settings body shared by project_defaults docs and pipeline docs.
 * Unknown keys are legal (passthrough) — hence the index signature.
 */
export interface PipelineSettings {
  serverless?: boolean
  edition?: string
  channel?: string
  continuous?: boolean
  packaging?: string
  catalog?: string
  schema?: string
  configuration?: Record<string, string>
  environment?: { dependencies?: string[]; [key: string]: unknown }
  permissions?: PipelinePermissionEntry[]
  clusters?: PipelineCluster[]
  notifications?: PipelineNotification[]
  tags?: Record<string, unknown>
  photon?: boolean
  /** Object form overrides lhp.yaml; `false` disables for this pipeline. */
  event_log?: PipelineEventLogOverride | false
  [key: string]: unknown
}

/** A `project_defaults` document. */
export interface PipelineDefaultsDoc {
  project_defaults: PipelineSettings
}

/** A pipeline document: `pipeline` (a list = the UI's "group") + settings. */
export interface PipelineDoc extends PipelineSettings {
  pipeline: string | string[]
}

export type PipelineDocKind = 'defaults' | 'pipeline' | 'unrecognized'

/**
 * Classify one document the way the loader's if/elif does
 * (pipeline_config_loader.py:137-144): `project_defaults` wins when both
 * keys are present (the `pipeline` key is then ignored — flagged as a
 * warning by `validatePipelineConfigFile`). Non-mapping docs and docs with
 * neither key are ignored by the loader → 'unrecognized'.
 */
export function classifyPipelineDoc(doc: unknown): PipelineDocKind {
  if (!isPlainObject(doc)) return 'unrecognized'
  if ('project_defaults' in doc) return 'defaults'
  if ('pipeline' in doc) return 'pipeline'
  return 'unrecognized'
}

/**
 * Keys of `doc` the pipeline form does NOT render (passthrough chips).
 * For a defaults doc these are read from inside `project_defaults`; for a
 * pipeline doc, from the doc root (excluding the `pipeline` key itself);
 * for an unrecognized mapping doc, every key (the loader ignores the doc).
 */
export function listPipelinePassthroughKeys(doc: unknown): string[] {
  const kind = classifyPipelineDoc(doc)
  if (kind === 'defaults') {
    const settings = (doc as Record<string, unknown>).project_defaults
    if (!isPlainObject(settings)) return []
    return Object.keys(settings).filter((key) => !PIPELINE_KNOWN_KEYS.has(key))
  }
  if (kind === 'pipeline') {
    return Object.keys(doc as Record<string, unknown>).filter(
      (key) => key !== 'pipeline' && !PIPELINE_KNOWN_KEYS.has(key),
    )
  }
  return isPlainObject(doc) ? Object.keys(doc) : []
}

/**
 * Validate all documents of a pipeline config file (plain-JS snapshots from
 * `toJS`, in document order). Mirrors `PipelineConfigLoader._load_config` +
 * `_validate_config`; `error` severities are exactly the loader's raises.
 */
export function validatePipelineConfigFile(docs: unknown[]): ValidationIssue[] {
  const issues: ValidationIssue[] = []
  /** name -> 1-based document number where first defined (loader `first_seen`). */
  const firstSeen = new Map<unknown, number>()

  docs.forEach((doc, docIndex) => {
    // Null documents are skipped silently (loader :130-131).
    if (doc === null || doc === undefined) return

    if (!isPlainObject(doc)) {
      // Loader :133-135 warns and ignores non-dict documents.
      issues.push(warningIssue(docIndex, [], `Document ${docIndex + 1} is not a mapping; the loader ignores it`))
      return
    }

    if ('project_defaults' in doc) {
      // Loader :137-142 reads ONLY doc["project_defaults"]; sibling keys
      // (including a `pipeline` key) are silently ignored.
      const ignored = Object.keys(doc).filter((key) => key !== 'project_defaults')
      if (ignored.length > 0) {
        issues.push(
          warningIssue(
            docIndex,
            [],
            `Document ${docIndex + 1}: 'project_defaults' takes precedence; the loader ignores: ${ignored.join(', ')}`,
          ),
        )
      }
      const settings = doc.project_defaults
      if (!isPlainObject(settings)) {
        // Not a mapping: the loader crashes on it (in `_validate_config` or
        // at merge time), so every CLI run fails — a blocking error.
        issues.push(errorIssue(docIndex, ['project_defaults'], `'project_defaults' must be a mapping`))
        return
      }
      validatePipelineSettings(settings, docIndex, ['project_defaults'], issues)
      return
    }

    if ('pipeline' in doc) {
      const raw = doc.pipeline
      let names: unknown[]
      let isScalar = false
      if (typeof raw === 'string') {
        names = [raw]
        isScalar = true
      } else if (Array.isArray(raw)) {
        names = raw
      } else {
        // Loader :151-156 warns and skips the WHOLE document before any
        // settings validation.
        issues.push(
          warningIssue(
            docIndex,
            ['pipeline'],
            `Document ${docIndex + 1} has an invalid 'pipeline' type (expected string or list); the loader skips it`,
          ),
        )
        return
      }

      if (names.length === 0) {
        issues.push(
          errorIssue(
            docIndex,
            ['pipeline'],
            `Document ${docIndex + 1} has an empty pipeline list; at least one pipeline name is required`,
            'VAL_005',
          ),
        )
        return
      }

      if (names.includes(MONITORING_PIPELINE_ALIAS) && names.length > 1) {
        // Loader :173-186 raises before registering any name of this doc.
        issues.push(
          errorIssue(
            docIndex,
            ['pipeline'],
            `'${MONITORING_PIPELINE_ALIAS}' must be a standalone pipeline entry, not part of a list`,
            'VAL_011',
          ),
        )
        return
      }

      const settings = Object.fromEntries(
        Object.entries(doc).filter(([key]) => key !== 'pipeline'),
      )
      validatePipelineSettings(settings, docIndex, [], issues)

      // Duplicate detection — loader :192-215: one `seen_pipelines` set
      // across ALL documents, group lists expanded, updated per name (so a
      // repeat within one list is also caught). The loader raises at the
      // first duplicate; the UI collects every occurrence.
      names.forEach((name, position) => {
        const path: (string | number)[] = isScalar ? ['pipeline'] : ['pipeline', position]
        const definedIn = firstSeen.get(name)
        if (definedIn !== undefined) {
          issues.push(
            errorIssue(
              docIndex,
              path,
              `pipeline '${String(name)}' in document ${docIndex + 1} was already defined in document ${definedIn}; each pipeline must be unique across all documents`,
              'VAL_006',
            ),
          )
        } else {
          firstSeen.set(name, docIndex + 1)
        }
      })
      return
    }

    // Loader :222-225 warns and ignores docs with neither key.
    issues.push(
      warningIssue(
        docIndex,
        [],
        `Document ${docIndex + 1} has neither 'project_defaults' nor 'pipeline'; the loader ignores it`,
      ),
    )
  })

  return issues
}

const IDENTITY_KEYS = ['user_name', 'group_name', 'service_principal_name'] as const

/** Field rules mirroring `_validate_config` (pipeline_config_loader.py:294-481). */
function validatePipelineSettings(
  config: Record<string, unknown>,
  docIndex: number,
  base: (string | number)[],
  issues: ValidationIssue[],
): void {
  if ('edition' in config && !isEnumMember(config.edition, PIPELINE_ALLOWED_EDITIONS)) {
    issues.push(
      errorIssue(
        docIndex,
        [...base, 'edition'],
        `Invalid edition '${String(config.edition)}'. Allowed values: ${[...PIPELINE_ALLOWED_EDITIONS].sort().join(', ')}`,
        'VAL_009',
      ),
    )
  }

  if ('channel' in config && !isEnumMember(config.channel, PIPELINE_ALLOWED_CHANNELS)) {
    issues.push(
      errorIssue(
        docIndex,
        [...base, 'channel'],
        `Invalid channel '${String(config.channel)}'. Allowed values: ${[...PIPELINE_ALLOWED_CHANNELS].sort().join(', ')}`,
        'VAL_009',
      ),
    )
  }

  if ('packaging' in config && !isEnumMember(config.packaging, PIPELINE_ALLOWED_PACKAGING_MODES)) {
    issues.push(
      errorIssue(
        docIndex,
        [...base, 'packaging'],
        `Invalid packaging '${String(config.packaging)}'. Allowed values: ${[...PIPELINE_ALLOWED_PACKAGING_MODES].sort().join(', ')}`,
        'VAL_062',
      ),
    )
  }

  if ('environment' in config && !isPlainObject(config.environment)) {
    issues.push(
      errorIssue(docIndex, [...base, 'environment'], `'environment' must be a mapping`, 'VAL_009'),
    )
  }

  if ('configuration' in config) {
    const configuration = config.configuration
    if (!isPlainObject(configuration)) {
      issues.push(
        errorIssue(docIndex, [...base, 'configuration'], `'configuration' must be a mapping`, 'VAL_009'),
      )
    } else {
      // Loader :381-400 HARD-FAILS on any non-string value ("All Databricks
      // pipeline configuration values must be strings"), so this is an
      // error, not a warning.
      for (const [key, value] of Object.entries(configuration)) {
        if (typeof value !== 'string') {
          issues.push(
            errorIssue(
              docIndex,
              [...base, 'configuration', key],
              `Configuration value for '${key}' must be a string (all pipeline configuration values must be quoted strings)`,
              'VAL_009',
            ),
          )
        }
      }
    }
  }

  if ('permissions' in config) {
    const permissions = config.permissions
    if (!Array.isArray(permissions)) {
      issues.push(
        errorIssue(
          docIndex,
          [...base, 'permissions'],
          `'permissions' must be a list of permission entries`,
          'VAL_009',
        ),
      )
      return
    }
    permissions.forEach((entry, index) => {
      const path = [...base, 'permissions', index]
      if (!isPlainObject(entry)) {
        issues.push(
          errorIssue(docIndex, path, `Permissions entry ${index} must be a mapping`, 'VAL_009'),
        )
        return
      }
      if (!('level' in entry) || typeof entry.level !== 'string') {
        issues.push(
          errorIssue(
            docIndex,
            path,
            `Permissions entry ${index} must have a string 'level' field (e.g. CAN_MANAGE, CAN_VIEW, CAN_RUN)`,
            'VAL_009',
          ),
        )
      }
      // Exactly one principal, by PRESENCE (loader :463-481 uses `k in entry`).
      const present = presentKeys(entry, IDENTITY_KEYS)
      if (present.length !== 1) {
        issues.push(
          errorIssue(
            docIndex,
            path,
            `Permissions entry ${index} must have exactly one of ${[...IDENTITY_KEYS].sort().join(', ')}; found: ${present.length === 0 ? 'none' : present.join(', ')}`,
            'VAL_009',
          ),
        )
      }
    })
  }
}
