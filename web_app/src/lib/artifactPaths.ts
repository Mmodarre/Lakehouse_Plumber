import type { FileNode } from '../types/api'

// ── artifactPaths — flowgroup → its code artifacts (§6.2 Code view) ──
//
// Pure, network-free resolver behind the multi-file Code view. Given a
// flowgroup's identity + the active env, it returns the ordered set of code
// artifacts to surface as sub-tabs: the editable source YAML, the generated
// Python (per-flowgroup file, falling back to the pipeline runner), and any
// source SQL / schema files the flowgroup references. Existence is decided
// against the caller-supplied file-tree paths so this stays trivially testable
// (the caller does the one `fetchFiles()` / related-files I/O).

export type ArtifactKind =
  | 'source-yaml'
  | 'generated-python'
  | 'source-sql'
  | 'source-schema'
  | 'source-python'
  | 'source-expectations'
  | 'source-tags'

export interface ArtifactRef {
  /** Project-relative path — unique per artifact (and per Monaco model). */
  path: string
  /** Basename shown on the sub-tab. */
  label: string
  /** Monaco language id derived from the extension. */
  language: string
  kind: ArtifactKind
  /** Only the source YAML is writable; every other artifact is read-only. */
  editable: boolean
  /** Muted chip text on the sub-tab, or null for the editable source. */
  chip: string | null
}

/** A file the flowgroup references (the subset of the related-files payload
 * this resolver needs — `RelatedFileInfo` is structurally assignable). */
export interface RelatedArtifact {
  path: string
  category: string
}

export interface ResolveArtifactsInput {
  /** '' for a template (no pipeline → no generated Python). */
  pipeline: string
  flowgroup: string
  /** Project-relative path of the editable source YAML. */
  sourceFilePath: string
  /** Active environment — scopes the `generated/<env>/…` lookup. */
  env: string
  /** Files the flowgroup references. The `sql`, `schema`, `python`,
   * `expectations`, and `tags` categories are surfaced (existence-checked);
   * anything else is ignored here. */
  related?: RelatedArtifact[]
}

const GENERATED_READ_ONLY = 'generated · read-only'
const READ_ONLY = 'read-only'

/** Related-file categories the extractor emits, mapped to the read-only
 * artifact kind that surfaces them as a tab. Categories absent here (if any)
 * are ignored. */
const RELATED_KIND: Record<string, ArtifactKind> = {
  sql: 'source-sql',
  schema: 'source-schema',
  python: 'source-python',
  expectations: 'source-expectations',
  tags: 'source-tags',
}

const EXT_TO_LANGUAGE: Record<string, string> = {
  yaml: 'yaml',
  yml: 'yaml',
  py: 'python',
  sql: 'sql',
  ddl: 'sql',
  json: 'json',
}

/** Monaco language id for a path (mirrors MonacoEditorWrapper's own mapping). */
export function languageForArtifact(path: string): string {
  const ext = path.split('.').pop()?.toLowerCase() ?? ''
  return EXT_TO_LANGUAGE[ext] ?? 'plaintext'
}

function basename(path: string): string {
  return path.split('/').pop() ?? path
}

/** Flatten the recursive file tree into the list of file (not directory)
 * paths. Safe on an absent tree (returns []). */
export function collectFilePaths(node: FileNode | undefined | null): string[] {
  if (!node) return []
  const out: string[] = []
  const walk = (n: FileNode): void => {
    if (n.type === 'file' && n.path) out.push(n.path)
    for (const child of n.children ?? []) walk(child)
  }
  walk(node)
  return out
}

/** Candidate generated-Python paths for a flowgroup, in preference order: the
 * per-flowgroup file first, then the pipeline runner fallback. Empty when
 * there is no env/pipeline (e.g. a template). */
export function generatedPythonCandidates(
  env: string,
  pipeline: string,
  flowgroup: string,
): string[] {
  if (!env || !pipeline) return []
  const base = `generated/${env}/${pipeline}`
  const runner = `${base}/${pipeline}_runner.py`
  return flowgroup ? [`${base}/${flowgroup}.py`, runner] : [runner]
}

/** Resolve a flowgroup's code artifacts for the active env against the set of
 * paths that actually exist on disk. Returns only artifacts that exist (the
 * source YAML is always included — the tab that hosts editing). */
export function resolveArtifactPaths(
  input: ResolveArtifactsInput,
  existingPaths: Iterable<string>,
): ArtifactRef[] {
  const existing =
    existingPaths instanceof Set ? existingPaths : new Set(existingPaths)
  const artifacts: ArtifactRef[] = []
  const seen = new Set<string>()

  const push = (ref: ArtifactRef): void => {
    if (seen.has(ref.path)) return
    seen.add(ref.path)
    artifacts.push(ref)
  }

  // 1. Editable source YAML — always first, always present.
  push({
    path: input.sourceFilePath,
    label: basename(input.sourceFilePath),
    language: languageForArtifact(input.sourceFilePath),
    kind: 'source-yaml',
    editable: true,
    chip: null,
  })

  // 2. Generated Python — the per-flowgroup file, else the pipeline runner.
  const py = generatedPythonCandidates(
    input.env,
    input.pipeline,
    input.flowgroup,
  ).find((p) => existing.has(p))
  if (py) {
    push({
      path: py,
      label: basename(py),
      language: 'python',
      kind: 'generated-python',
      editable: false,
      chip: GENERATED_READ_ONLY,
    })
  }

  // 3. Referenced source files (sql / schema / python / expectations / tags)
  //    that actually exist — project sources shown read-only alongside the
  //    editable yaml and the generated python.
  for (const rel of input.related ?? []) {
    const kind = RELATED_KIND[rel.category]
    if (!kind) continue
    if (!existing.has(rel.path)) continue
    push({
      path: rel.path,
      label: basename(rel.path),
      language: languageForArtifact(rel.path),
      kind,
      editable: false,
      chip: READ_ONLY,
    })
  }

  return artifacts
}
