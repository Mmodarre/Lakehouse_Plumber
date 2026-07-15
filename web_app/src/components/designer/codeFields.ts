// ── codeFields — which action fields hold editable code ──────
//
// The designer offers a Monaco code modal on the SQL / Python / expectations
// fields. Those fields are identified by the LAST segment of their YAML path
// — the same well-known keys across every spec (transform `sql`/`sql_path`,
// load `source.sql`/`source.sql_path`/`source.module_path`, write_target
// `sql`/`sql_path`/`module_path`, `expectations_file`, custom-sql test `sql`).
// Keeping this as an engine-level rule (rather than a per-spec flag) means a
// new spec that names one of these keys gets the affordance for free, with no
// change to the spec vocabulary.
//
// `backing` distinguishes an INLINE body (the value IS the code, edited back
// into the YAML) from a FILE ref (the value is a project-relative path whose
// content is edited through the files API). For inline bodies we also carry
// the Monaco language; file refs derive it from the path extension.

import type { YamlPath } from '@/lib/flowgroup-doc'
import type { FieldSpec } from './specs/types'

export type CodeLanguage = 'sql' | 'python' | 'yaml'

export interface CodeFieldInfo {
  backing: 'inline' | 'file'
  /** Monaco language for an INLINE body (ignored for file refs). */
  inlineLanguage: CodeLanguage
}

/** Keyed by the field path's last segment. */
const BY_LAST_SEGMENT: Record<string, CodeFieldInfo> = {
  sql: { backing: 'inline', inlineLanguage: 'sql' },
  sql_path: { backing: 'file', inlineLanguage: 'sql' },
  module_path: { backing: 'file', inlineLanguage: 'python' },
  expectations_file: { backing: 'file', inlineLanguage: 'sql' },
  schema_inline: { backing: 'inline', inlineLanguage: 'yaml' },
  schema_file: { backing: 'file', inlineLanguage: 'yaml' },
}

/** Extension allow-list for a file ref synthesized from a code language. */
const ACCEPT_BY_LANGUAGE: Record<CodeLanguage, string[]> = {
  sql: ['.sql'],
  python: ['.py'],
  yaml: ['.yaml', '.yml'],
}

/** Code-field info for a field path, or `null` when the field is not code. */
export function codeFieldForPath(path: YamlPath): CodeFieldInfo | null {
  const last = path[path.length - 1]
  if (typeof last !== 'string') return null
  return BY_LAST_SEGMENT[last] ?? null
}

// `fileRefForField` lives here (not beside `FieldSpec`) because it is a
// code/file-ref concern: it owns `BY_LAST_SEGMENT` and the language→accept
// mapping. There is no import cycle — `specs/types.ts` does not import this
// module, so importing the `FieldSpec` type here is one-directional.
/**
 * Resolve whether a field is a project-relative FILE reference, returning the
 * accept allow-list (and optional `baseDir`) to drive a file control, or
 * `null` when it is not. An explicit `spec.fileRef` wins as-is; otherwise the
 * last-segment heuristic applies — but ONLY for FILE-backed entries. An inline
 * body (e.g. `sql`) is editable code, not a file reference, so it returns
 * `null`.
 */
export function fileRefForField(
  spec: FieldSpec,
): { accept: string[]; baseDir?: string } | null {
  if (spec.fileRef) return spec.fileRef
  const info = codeFieldForPath(spec.path)
  if (info?.backing !== 'file') return null
  return { accept: ACCEPT_BY_LANGUAGE[info.inlineLanguage] }
}
