/**
 * View-model + pure client-side validation for job_config*.yaml (and
 * monitoring_job_config*.yaml, which uses the flat single-doc shape).
 *
 * Ground truth: `src/lhp/core/loaders/job_config_loader.py` (single- vs
 * multi-document handling, VAL_003/VAL_004) and
 * `src/lhp/core/jobs/job_generator.py` (DEFAULT_JOB_CONFIG :60-66,
 * EXPLICITLY_RENDERED_JOB_CONFIG_KEYS :38-56). The loader does NO per-field
 * validation — unknown keys are rendered verbatim into the job YAML — so
 * this validator errors only on the loader's raises and keeps everything
 * else permissive (shape hints are warnings at most).
 */
import type { ValidationIssue } from './validators'
import { errorIssue, isPlainObject, warningIssue } from './validators'

/** Built-in defaults (lowest merge layer) — job_generator.py:60-66. */
export const JOB_BUILTIN_DEFAULTS = {
  max_concurrent_runs: 1,
  queue: { enabled: true },
  performance_target: 'STANDARD',
  generate_master_job: true,
  master_job_name: null,
} as const

/**
 * Keys the job form renders as first-class fields — exactly
 * EXPLICITLY_RENDERED_JOB_CONFIG_KEYS (job_generator.py:38-56). Everything
 * else (trigger, run_as, git_source, health, continuous, environments, …)
 * is passthrough: rendered verbatim by the `toyaml` filter.
 */
export const JOB_KNOWN_KEYS: ReadonlySet<string> = new Set([
  'max_concurrent_runs',
  'queue',
  'performance_target',
  'timeout_seconds',
  'tags',
  'email_notifications',
  'webhook_notifications',
  'permissions',
  'schedule',
  'notebook_cluster', // only rendered by monitoring_job_resource.yml.j2
  'generate_master_job', // LHP control knob, never emitted
  'master_job_name', // LHP control knob, never emitted
])

/** schedule block (templates/bundle/job_resource.yml.j2:91-95). */
export interface JobSchedule {
  quartz_cron_expression?: string
  timezone_id?: string
  pause_status?: string
  [key: string]: unknown
}

/** email_notifications block (job_resource.yml.j2:37-57). */
export interface JobEmailNotifications {
  on_start?: string[]
  on_success?: string[]
  on_failure?: string[]
  [key: string]: unknown
}

/** webhook_notifications block (job_resource.yml.j2:58-78). */
export interface JobWebhookNotifications {
  on_start?: { id?: string }[]
  on_success?: { id?: string }[]
  on_failure?: { id?: string }[]
  [key: string]: unknown
}

/** One permissions[] entry (job_resource.yml.j2:79-89 — not validated by the loader). */
export interface JobPermissionEntry {
  level?: string
  user_name?: string
  group_name?: string
  service_principal_name?: string
  [key: string]: unknown
}

/** notebook_cluster overrides (monitoring job only). */
export interface JobNotebookCluster {
  spark_version?: string
  node_type_id?: string
  num_workers?: number
  [key: string]: unknown
}

/**
 * The settings body shared by project_defaults docs, job docs, and legacy
 * flat docs. Unknown keys are legal (passthrough).
 */
export interface JobSettings {
  max_concurrent_runs?: number
  queue?: { enabled?: boolean; [key: string]: unknown }
  performance_target?: string
  timeout_seconds?: number
  tags?: Record<string, unknown>
  email_notifications?: JobEmailNotifications
  webhook_notifications?: JobWebhookNotifications
  permissions?: JobPermissionEntry[]
  schedule?: JobSchedule
  notebook_cluster?: JobNotebookCluster
  generate_master_job?: boolean
  master_job_name?: string | null
  [key: string]: unknown
}

/** A `project_defaults` document. */
export interface JobDefaultsDoc {
  project_defaults: JobSettings
}

/** A job-specific document: `job_name` (string or list) + settings. */
export interface JobDoc extends JobSettings {
  job_name: string | string[]
}

export type JobDocKind = 'defaults' | 'job' | 'legacy-flat' | 'unrecognized'

/**
 * Classify one document. `documentCount` is the number of NON-NULL documents
 * in the file (the loader drops null docs before deciding,
 * job_config_loader.py:91) and is required because classification is
 * file-shape dependent:
 *
 * - `project_defaults` present → 'defaults' (any count, loader :99-101/:114).
 * - single-document file WITHOUT `project_defaults` → 'legacy-flat': the
 *   loader treats the WHOLE doc as project defaults (:102-106) — even when
 *   it contains a `job_name` key, which the single-doc path never reads.
 *   The UI shows its convert-to-multi-doc banner off this kind.
 * - multi-document file: `job_name` present → 'job' (:118); neither key →
 *   'unrecognized' (skipped with a warning, :180-184).
 */
export function classifyJobDoc(doc: unknown, documentCount: number): JobDocKind {
  if (!isPlainObject(doc)) return 'unrecognized'
  if ('project_defaults' in doc) return 'defaults'
  if (documentCount <= 1) return 'legacy-flat'
  if ('job_name' in doc) return 'job'
  return 'unrecognized'
}

/**
 * Keys of `doc` the job form does NOT render (passthrough chips). Defaults
 * docs are read inside `project_defaults`; job docs at the root excluding
 * `job_name`; legacy flat docs at the root.
 */
export function listJobPassthroughKeys(doc: unknown, documentCount: number): string[] {
  const kind = classifyJobDoc(doc, documentCount)
  if (kind === 'unrecognized') return isPlainObject(doc) ? Object.keys(doc) : []
  if (kind === 'defaults') {
    const settings = (doc as Record<string, unknown>).project_defaults
    if (!isPlainObject(settings)) return []
    return Object.keys(settings).filter((key) => !JOB_KNOWN_KEYS.has(key))
  }
  const excludeJobName = kind === 'job'
  return Object.keys(doc as Record<string, unknown>).filter(
    (key) => !(excludeJobName && key === 'job_name') && !JOB_KNOWN_KEYS.has(key),
  )
}

/**
 * Validate all documents of a job config file (plain-JS snapshots from
 * `toJS`, in document order — nulls included so indices line up).
 * Mirrors `JobConfigLoader.load`; `error` severities are the loader's
 * raises (VAL_003 empty job_name list, VAL_004 duplicate job_name) plus
 * shapes the loader crashes on. Everything else is at most a warning.
 */
export function validateJobConfigFile(docs: unknown[]): ValidationIssue[] {
  const issues: ValidationIssue[] = []
  // The loader filters null docs BEFORE the single/multi decision (:91), so
  // [null, {job doc}] is a single-document (legacy) file.
  const present = docs
    .map((doc, docIndex) => ({ doc, docIndex }))
    .filter(({ doc }) => doc !== null && doc !== undefined)

  if (present.length === 0) return issues // empty file → defaults (:93-95)

  if (present.length === 1) {
    const { doc, docIndex } = present[0]
    if (!isPlainObject(doc)) {
      // The single-doc path assumes a mapping; anything else crashes the
      // loader or the merge — a blocking error.
      issues.push(errorIssue(docIndex, [], `Document ${docIndex + 1} must be a mapping`))
      return issues
    }
    if ('project_defaults' in doc) {
      validateDefaultsDoc(doc, docIndex, issues)
    } else {
      // Legacy flat doc: accepted wholesale as project defaults (:102-106).
      collectScheduleWarnings(doc, docIndex, [], issues)
    }
    return issues
  }

  /** name -> 1-based document number where first defined (loader `first_seen`). */
  const firstSeen = new Map<unknown, number>()

  for (const { doc, docIndex } of present) {
    if (!isPlainObject(doc)) {
      issues.push(
        warningIssue(docIndex, [], `Document ${docIndex + 1} is not a mapping; the loader skips it`),
      )
      continue
    }

    if ('project_defaults' in doc) {
      validateDefaultsDoc(doc, docIndex, issues)
      continue
    }

    if ('job_name' in doc) {
      const raw = doc.job_name
      let names: unknown[]
      let isScalar = false
      if (typeof raw === 'string') {
        names = [raw]
        isScalar = true
      } else if (Array.isArray(raw)) {
        names = raw
      } else {
        // Loader :126-130 warns and skips the document.
        issues.push(
          warningIssue(
            docIndex,
            ['job_name'],
            `Document ${docIndex + 1} has an invalid 'job_name' type (expected string or list); the loader skips it`,
          ),
        )
        continue
      }

      if (names.length === 0) {
        issues.push(
          errorIssue(
            docIndex,
            ['job_name'],
            `Document ${docIndex + 1} has an empty job_name list; at least one job name is required`,
            'VAL_003',
          ),
        )
        continue
      }

      collectScheduleWarnings(doc, docIndex, [], issues)

      // Duplicates — loader :149-169: one `seen_job_names` set across all
      // documents, lists expanded, updated per name. The loader raises at
      // the first duplicate; the UI collects every occurrence.
      names.forEach((name, position) => {
        const path: (string | number)[] = isScalar ? ['job_name'] : ['job_name', position]
        const definedIn = firstSeen.get(name)
        if (definedIn !== undefined) {
          issues.push(
            errorIssue(
              docIndex,
              path,
              `job_name '${String(name)}' in document ${docIndex + 1} was already defined in document ${definedIn}; each job_name must be unique across all documents`,
              'VAL_004',
            ),
          )
        } else {
          firstSeen.set(name, docIndex + 1)
        }
      })
      continue
    }

    // Loader :180-184 warns and skips docs with neither key.
    issues.push(
      warningIssue(
        docIndex,
        [],
        `Document ${docIndex + 1} has neither 'project_defaults' nor 'job_name'; the loader skips it`,
      ),
    )
  }

  return issues
}

/** Shared handling for a defaults doc: sibling keys are ignored by the loader. */
function validateDefaultsDoc(
  doc: Record<string, unknown>,
  docIndex: number,
  issues: ValidationIssue[],
): void {
  const ignored = Object.keys(doc).filter((key) => key !== 'project_defaults')
  if (ignored.length > 0) {
    issues.push(
      warningIssue(
        docIndex,
        [],
        `Document ${docIndex + 1}: only 'project_defaults' is read; the loader ignores: ${ignored.join(', ')}`,
      ),
    )
  }
  const settings = doc.project_defaults
  if (!isPlainObject(settings)) {
    // A non-mapping project_defaults crashes the generator's deep merge —
    // every `lhp generate` run fails, so this blocks Save.
    issues.push(errorIssue(docIndex, ['project_defaults'], `'project_defaults' must be a mapping`))
    return
  }
  collectScheduleWarnings(settings, docIndex, ['project_defaults'], issues)
}

const SCHEDULE_STRING_FIELDS = ['quartz_cron_expression', 'timezone_id', 'pause_status'] as const

/**
 * Shape hints for `schedule` — WARNINGS only. The Python loader performs no
 * field validation at all, so blocking Save here would be stricter than the
 * loader; but the template renders schedule.* as scalars
 * (templates/bundle/job_resource.yml.j2:91-95), so a wrong shape produces a
 * broken job YAML worth flagging.
 */
function collectScheduleWarnings(
  settings: Record<string, unknown>,
  docIndex: number,
  base: (string | number)[],
  issues: ValidationIssue[],
): void {
  if (!('schedule' in settings)) return
  const schedule = settings.schedule
  if (!isPlainObject(schedule)) {
    issues.push(
      warningIssue(
        docIndex,
        [...base, 'schedule'],
        `'schedule' should be a mapping with quartz_cron_expression, timezone_id, and pause_status`,
      ),
    )
    return
  }
  for (const field of SCHEDULE_STRING_FIELDS) {
    if (field in schedule && typeof schedule[field] !== 'string') {
      issues.push(
        warningIssue(
          docIndex,
          [...base, 'schedule', field],
          `'schedule.${field}' should be a string`,
        ),
      )
    }
  }
}
