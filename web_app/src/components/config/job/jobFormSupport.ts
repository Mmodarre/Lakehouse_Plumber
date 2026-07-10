import type { ValidationIssue } from '../../../lib/config-model'
import { classifyJobDoc, isPlainObject, JOB_KNOWN_KEYS } from '../../../lib/config-model'
import type { JobDocKind } from '../../../lib/config-model'
import type { ConfigFileHandle } from '../../../lib/yaml-doc'
import { getPath, setPath } from '../../../lib/yaml-doc'
import type { DocFormApi } from '../shared/docFormSupport'

// ── jobFormSupport — job rail modelling + file-mode logic ────
//
// Pure helpers behind JobConfigEditor: which of the three job-file shapes
// this file is (standard multi-doc / legacy flat / monitoring flat), the
// rail display model with the duplicate-job_name matrix, and the explicit
// legacy→multi-doc conversion. The generic write funnel lives in
// ../shared/docFormSupport.

/** Documents the loader actually sees (it drops nulls before deciding). */
export function presentDocs(docs: unknown[]): { doc: unknown; index: number }[] {
  return docs
    .map((doc, index) => ({ doc, index }))
    .filter(({ doc }) => doc !== null && doc !== undefined)
}

/**
 * Monitoring job configs are detected by file name (`monitoring_job_config*`
 * — the same convention the picker/template flow uses). Their loader
 * (monitoring_service.py) is a different code path from JobConfigLoader:
 * `yaml.safe_load` (single document only), whole mapping = the settings,
 * no `project_defaults` wrapper, no `job_name`.
 */
export function isMonitoringJobConfigPath(path: string | null): boolean {
  const name = (path ?? '').split('/').pop() ?? ''
  return name.startsWith('monitoring_job_config')
}

export type JobFileMode = 'standard' | 'legacy' | 'monitoring'

/** Which editor layout this file gets (see JobConfigEditor). */
export function jobFileMode(path: string | null, docs: unknown[]): JobFileMode {
  if (isMonitoringJobConfigPath(path)) return 'monitoring'
  const present = presentDocs(docs)
  if (present.length === 1 && classifyJobDoc(present[0]!.doc, 1) === 'legacy-flat') {
    return 'legacy'
  }
  return 'standard'
}

/** Job names of one doc as display strings ([] for non-job docs). */
function jobNamesOf(doc: unknown): string[] {
  if (!isPlainObject(doc)) return []
  const raw = doc.job_name
  if (typeof raw === 'string') return [raw]
  if (Array.isArray(raw)) return raw.map((name) => String(name))
  return []
}

/**
 * Names that appear more than once across ALL job docs (lists expanded; a
 * repeat within one list counts too). Unlike the validator — which, like
 * the loader (VAL_004), flags only the LATER registrations — every
 * occurrence of a clashing name is included, so the rail can badge EVERY
 * involved document.
 */
export function duplicateJobNames(docs: unknown[]): ReadonlySet<string> {
  const count = presentDocs(docs).length
  const counts = new Map<string, number>()
  for (const doc of docs) {
    if (classifyJobDoc(doc, count) !== 'job') continue
    for (const name of jobNamesOf(doc)) counts.set(name, (counts.get(name) ?? 0) + 1)
  }
  return new Set([...counts].filter(([, n]) => n > 1).map(([name]) => name))
}

/** One rail row's display model. */
export interface JobRailDoc {
  index: number
  kind: JobDocKind
  /** Job doc whose `job_name` key is a LIST (the UI's "group"). */
  isGroup: boolean
  label: string
  caption?: string
  names: string[]
  errors: number
  warnings: number
  /** Some name in this doc clashes with another doc (or repeats in-list). */
  duplicate: boolean
}

/** Build the rail's display model (file order preserved; standard mode only). */
export function buildJobRailDocs(docs: unknown[], issues: ValidationIssue[]): JobRailDoc[] {
  const count = presentDocs(docs).length
  const duplicates = duplicateJobNames(docs)
  return docs.map((doc, index) => {
    const kind = classifyJobDoc(doc, count)
    const names = jobNamesOf(doc)
    const docIssues = issues.filter((issue) => issue.docIndex === index)
    const errors = docIssues.filter((issue) => issue.severity === 'error').length
    const warnings = docIssues.length - errors

    const isGroup = kind === 'job' && Array.isArray((doc as Record<string, unknown>).job_name)

    let label: string
    let caption: string | undefined
    if (kind === 'defaults') {
      label = 'Project defaults'
      caption = 'applied to every job'
    } else if (kind === 'job') {
      if (isGroup) {
        label = `${names.length} job${names.length === 1 ? '' : 's'}`
        caption = names.length > 0 ? names.join(', ') : 'empty group'
      } else {
        label = names[0] !== undefined && names[0] !== '' ? names[0] : 'Unnamed job'
        caption = 'single job'
      }
    } else {
      label = 'Unrecognized document'
      caption = 'not a job document'
    }

    return {
      index,
      kind,
      isGroup,
      label,
      caption,
      names,
      errors,
      warnings,
      duplicate: names.some((name) => duplicates.has(name)),
    }
  })
}

/**
 * Passthrough keys of a FLAT settings doc (monitoring / legacy): root keys
 * the form does not render. Bypasses listJobPassthroughKeys because a
 * monitoring doc that (wrongly) contains `project_defaults` must still be
 * read flat — the monitoring loader never unwraps it.
 */
export function flatPassthroughKeys(doc: unknown): string[] {
  if (!isPlainObject(doc)) return []
  return Object.keys(doc).filter((key) => !JOB_KNOWN_KEYS.has(key))
}

/**
 * Delete `settings[parentKey][childKey]` with pristine-absence cascade:
 * when `childKey` is the parent mapping's LAST key, the parent key itself
 * is deleted, so the file goes back to not mentioning the block at all
 * (`schedule`, `queue`, `email_notifications`, … must never linger as
 * `key: {}` residue). Parent keys holding non-mapping values are left to
 * the caller's shape guards.
 */
export function delWithCascade(api: DocFormApi, parentKey: string, childKey: string): void {
  const parent = api.settings[parentKey]
  if (isPlainObject(parent) && childKey in parent) {
    const remaining = Object.keys(parent).filter((key) => key !== childKey)
    if (remaining.length === 0) {
      api.del([parentKey])
      return
    }
  }
  api.del([parentKey, childKey])
}

/**
 * Rewrite a legacy flat job_config document into the multi-document
 * layout: the whole top-level mapping moves under `project_defaults:`.
 *
 * Loader equivalence (job_config_loader.py): for a single-document file
 * the loader returns `doc["project_defaults"]` when that key is present
 * (:99-101) and the whole `doc` otherwise (:102-106) — both produce the
 * same `(project_defaults, {})` tuple, so this wrap is semantically a
 * no-op for `lhp`. It IS a structural rewrite of the document's text
 * (yaml-doc's rewrite class): comments travel with their keys but get
 * re-indented, and whitespace is normalized — which is why this only ever
 * runs from the explicit convert dialog, never on save.
 */
export function convertLegacyJobDoc(handle: ConfigFileHandle, docIndex: number): void {
  // getPath([]) yields the document's root AST node; re-using the node
  // (not a plain-JS snapshot) carries key order and per-entry comments
  // through the rewrite.
  setPath(handle, docIndex, [], { project_defaults: getPath(handle, docIndex, []) })
}
