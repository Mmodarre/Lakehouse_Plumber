import type { UseConfigFileResult } from '../../../hooks/useConfigFile'
import type { ValidationIssue } from '../../../lib/config-model'
import type { YamlPath } from '../../../lib/yaml-doc'
import { addDocument, deletePath, documentCount, getPath, setPath } from '../../../lib/yaml-doc'

// ── projectFormSupport — write plumbing shared by the sections ──
//
// The section components never touch yaml-doc directly: they go through
// this ProjectFormApi so the byte-preservation rules live in ONE place —
// edits are `setPath` scalar patches / new-key splices, removals are
// `deletePath` (pristine absence), and the two lhp.yaml oddities are
// handled centrally (a file with no YAML document yet, and a section
// present as a bare `section:` null that must become a map on first
// field write, because setIn cannot descend into a null scalar).

export interface ProjectFormApi {
  /** toJS snapshot of document 0 — `{}` when the file is empty. */
  doc: Record<string, unknown>
  /** All validation issues for the document (config-model). */
  issues: ValidationIssue[]
  /** Surgical set at `path` in doc 0 (creates the document when the file is empty). */
  set: (path: YamlPath, value: unknown) => void
  /**
   * Set `key` under `base`, tolerating a bare-null section: when the node
   * at `base` is `null` (e.g. `monitoring:` with no value), the whole
   * section is replaced by `{key: value}` instead of descending into it.
   */
  setField: (base: YamlPath, key: string, value: unknown) => void
  /** Delete exactly the node at `path` (no-op when absent). */
  del: (path: YamlPath) => void
}

/** Build the write API over a loaded useConfigFile instance. */
export function buildProjectFormApi(
  file: UseConfigFileResult,
  doc: Record<string, unknown>,
  issues: ValidationIssue[],
): ProjectFormApi {
  const run = (fn: Parameters<UseConfigFileResult['mutate']>[0]) =>
    file.mutate((handle) => {
      if (documentCount(handle) === 0) addDocument(handle, {})
      fn(handle)
    })
  return {
    doc,
    issues,
    set: (path, value) => run((h) => setPath(h, 0, path, value)),
    setField: (base, key, value) =>
      run((h) => {
        if (base.length > 0 && getPath(h, 0, base) === null) {
          setPath(h, 0, base, { [key]: value })
        } else {
          setPath(h, 0, [...base, key], value)
        }
      }),
    del: (path) =>
      file.mutate((h) => {
        if (documentCount(h) > 0) deletePath(h, 0, path)
      }),
  }
}

function samePath(a: readonly (string | number)[], b: readonly (string | number)[]): boolean {
  return a.length === b.length && a.every((seg, i) => seg === b[i])
}

/** All issues whose path is exactly `path`. */
export function issuesAtExactly(
  issues: ValidationIssue[],
  path: readonly (string | number)[],
): ValidationIssue[] {
  return issues.filter((issue) => samePath(issue.path, path))
}

/** Message of the worst issue at exactly `path` (errors outrank warnings). */
export function issueText(
  issues: ValidationIssue[],
  path: readonly (string | number)[],
): { message: string; severity: 'error' | 'warning' } | undefined {
  const at = issuesAtExactly(issues, path)
  if (at.length === 0) return undefined
  const error = at.find((issue) => issue.severity === 'error')
  const picked = error ?? at[0]!
  return { message: picked.message, severity: picked.severity }
}

/** Count of blocking issues (Save is disabled while > 0). */
export function countErrors(issues: ValidationIssue[]): number {
  return issues.reduce((n, issue) => (issue.severity === 'error' ? n + 1 : n), 0)
}
