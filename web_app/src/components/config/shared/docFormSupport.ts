import type { YAMLError } from 'yaml'
import type { ValidationIssue } from '../../../lib/config-model'
import type { ConfigFileHandle, YamlPath } from '../../../lib/yaml-doc'
import { deletePath, documentCount, getPath, setPath, toJS } from '../../../lib/yaml-doc'

// ── ConfigDocSource — documentStore-backed funnel for the config forms ──
//
// The config editors and their write plumbing were built against
// `useConfigFile`'s result. That hook (and its parallel save / conflict /
// external-change lifecycle) is gone: the entity document core
// (store/documentStore) now owns the parse handle, dirty derives from the
// workspace buffer, and saving is the buffer's ⌘S path. This is the narrow
// slice the forms actually read — a loaded handle keyed by a monotonically
// bumped `version`, plus a one-op mutate funnel (each field commit = one
// atomic yaml-doc op routed through `documentStore.mutate`). ConfigFormView
// builds one of these over `useEntityDocument`.
export interface ConfigDocSource {
  /** Project-relative path this source edits (`null` = nothing loaded). */
  path: string | null
  /** First content load still in flight. */
  isLoading: boolean
  /** Load failed — user-facing message, else `null`. */
  loadError: string | null
  /** Comment-preserving handle; `null` until the buffer is parsed. */
  handle: ConfigFileHandle | null
  /** Parse errors on the current handle; non-empty ⇒ degraded (no mutate). */
  errors: readonly YAMLError[]
  /** Bumped on every mutation/reparse — the ONLY memo key (never memo on the
   * handle, whose identity is preserved across mutate). */
  version: number
  /** Run one atomic yaml-doc op on the handle (byte-surgical). No-op when the
   * document is absent, degraded (parse errors), or read-only (viewer). */
  mutate: (fn: (handle: ConfigFileHandle) => void) => void
}

// ── docFormSupport — the multi-document write funnel ─────────
//
// Shared by the pipeline and job editors (extracted from
// pipelineFormSupport in Task 9): the form components never touch
// yaml-doc directly — every write goes through a DocFormApi bound to one
// document (and, for defaults docs, to its settings base path), so the
// byte-preservation rules live in one place. What stays surface-specific
// (doc classification, rail models, duplicate matrices) lives in
// pipeline/pipelineFormSupport and job/jobFormSupport.

/** Rail selection: the built-in ghost row or a document index. */
export type RailSelection = 'builtin' | number

/** toJS snapshots of every document, in file order. */
export function snapshotDocs(file: ConfigDocSource): unknown[] {
  if (file.handle === null || file.errors.length > 0) return []
  const count = documentCount(file.handle)
  const docs: unknown[] = []
  for (let i = 0; i < count; i++) docs.push(toJS(file.handle, i))
  return docs
}

/** Count of blocking issues (Save is disabled while > 0). */
export function countErrors(issues: ValidationIssue[]): number {
  return issues.reduce((n, issue) => (issue.severity === 'error' ? n + 1 : n), 0)
}

function samePath(a: readonly (string | number)[], b: readonly (string | number)[]): boolean {
  return a.length === b.length && a.every((seg, i) => seg === b[i])
}

/** All issues of one document whose path is exactly `path`. */
export function issuesAtExactly(
  issues: ValidationIssue[],
  docIndex: number,
  path: readonly (string | number)[],
): ValidationIssue[] {
  return issues.filter((issue) => issue.docIndex === docIndex && samePath(issue.path, path))
}

/** Message of the worst issue at exactly `path` (errors outrank warnings). */
function issueTextAt(
  issues: ValidationIssue[],
  docIndex: number,
  path: readonly (string | number)[],
): { message: string; severity: 'error' | 'warning' } | undefined {
  const at = issuesAtExactly(issues, docIndex, path)
  if (at.length === 0) return undefined
  const error = at.find((issue) => issue.severity === 'error')
  const picked = error ?? at[0]!
  return { message: picked.message, severity: picked.severity }
}

/**
 * Write/read API bound to ONE document (and its settings base path).
 *
 * `base` is `['project_defaults']` for defaults docs and `[]` for
 * pipeline/job docs, so the settings sections address fields the same way
 * on both. All paths passed to `set`/`del`/`issueAt` are RELATIVE to
 * `base`.
 */
export interface DocFormApi {
  docIndex: number
  base: YamlPath
  /** Snapshot of the settings mapping at `base` (`{}` when absent/null). */
  settings: Record<string, unknown>
  /** Surgical set at base+rel (yaml-doc patch/splice rules apply). */
  set: (rel: YamlPath, value: unknown) => void
  /** Delete exactly base+rel (no-op when absent). */
  del: (rel: YamlPath) => void
  /** Worst validation issue at exactly base+rel. */
  issueAt: (
    rel: YamlPath,
  ) => { message: string; severity: 'error' | 'warning' } | undefined
}

/** `{a: {b: value}}` from `['a','b']` — spine for bare-null replacement. */
function nestValue(keys: readonly string[], value: unknown): unknown {
  return [...keys].reverse().reduce<unknown>((acc, key) => ({ [key]: acc }), value)
}

/** Bind a DocFormApi over a loaded config document source. */
export function bindDocApi(
  file: ConfigDocSource,
  docIndex: number,
  base: YamlPath,
  settings: Record<string, unknown>,
  issues: ValidationIssue[],
): DocFormApi {
  const abs = (rel: YamlPath): YamlPath => [...base, ...rel]
  return {
    docIndex,
    base,
    settings,
    set: (rel, value) =>
      file.mutate((handle) => {
        if (
          base.length > 0 &&
          getPath(handle, docIndex, base) === null &&
          rel.every((seg) => typeof seg === 'string')
        ) {
          // Bare `project_defaults:` (null scalar) — setIn cannot descend
          // into it; replace the section with a map spine instead.
          setPath(handle, docIndex, base, nestValue(rel as string[], value))
        } else {
          setPath(handle, docIndex, abs(rel), value)
        }
      }),
    del: (rel) => file.mutate((handle) => deletePath(handle, docIndex, abs(rel))),
    issueAt: (rel) => issueTextAt(issues, docIndex, abs(rel)),
  }
}
