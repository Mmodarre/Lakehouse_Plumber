import type { ValidationIssue } from '../../../lib/config-model'
import { classifyPipelineDoc, isPlainObject } from '../../../lib/config-model'
import type { PipelineDocKind } from '../../../lib/config-model'

// ── pipelineFormSupport — pipeline rail modelling ────────────
//
// Pure rail-model helpers (labels, duplicate matrix, per-doc issue
// counts) so PipelineDocList stays presentational. The generic
// multi-document write funnel (DocFormApi/bindDocApi, snapshots, issue
// lookups) lives in ../shared/docFormSupport, shared with the job editor.

/** Pipeline names of one doc as display strings ([] for non-pipeline docs). */
function pipelineNamesOf(doc: unknown): string[] {
  if (!isPlainObject(doc)) return []
  const raw = doc.pipeline
  if (typeof raw === 'string') return [raw]
  if (Array.isArray(raw)) return raw.map((name) => String(name))
  return []
}

/**
 * Names that appear more than once across ALL pipeline docs (group lists
 * expanded; a repeat within one list counts too). Unlike the validator —
 * which, like the loader, flags only the LATER registrations — every
 * occurrence of a clashing name is included, so the rail can badge EVERY
 * involved document.
 */
export function duplicateNames(docs: unknown[]): ReadonlySet<string> {
  const counts = new Map<string, number>()
  for (const doc of docs) {
    if (classifyPipelineDoc(doc) !== 'pipeline') continue
    for (const name of pipelineNamesOf(doc)) counts.set(name, (counts.get(name) ?? 0) + 1)
  }
  return new Set([...counts].filter(([, count]) => count > 1).map(([name]) => name))
}

/** One rail row's display model. */
export interface RailDoc {
  index: number
  kind: PipelineDocKind
  /** Pipeline doc whose `pipeline` key is a LIST (the UI's "group"). */
  isGroup: boolean
  label: string
  caption?: string
  names: string[]
  errors: number
  warnings: number
  /** Some name in this doc clashes with another doc (or repeats in-list). */
  duplicate: boolean
}

/** Build the rail's display model (file order preserved). */
export function buildRailDocs(docs: unknown[], issues: ValidationIssue[]): RailDoc[] {
  const duplicates = duplicateNames(docs)
  return docs.map((doc, index) => {
    const kind = classifyPipelineDoc(doc)
    const names = pipelineNamesOf(doc)
    const docIssues = issues.filter((issue) => issue.docIndex === index)
    const errors = docIssues.filter((issue) => issue.severity === 'error').length
    const warnings = docIssues.length - errors

    const isGroup =
      kind === 'pipeline' && Array.isArray((doc as Record<string, unknown>).pipeline)

    let label: string
    let caption: string | undefined
    if (kind === 'defaults') {
      label = 'Project defaults'
      caption = 'applied to every pipeline'
    } else if (kind === 'pipeline') {
      if (isGroup) {
        label = `${names.length} pipeline${names.length === 1 ? '' : 's'}`
        caption = names.length > 0 ? names.join(', ') : 'empty group'
      } else {
        label = names[0] !== undefined && names[0] !== '' ? names[0] : 'Unnamed pipeline'
        caption = 'single pipeline'
      }
    } else {
      label = 'Unrecognized document'
      caption = 'not a pipeline document'
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
