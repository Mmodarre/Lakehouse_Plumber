import type { FileNode, TableSummary } from '../../../types/api'

// ── explorerData — pure helpers for the multi-lens explorer (T1.3) ──
//
// Framework-free logic behind the Structure / Tables lenses: width clamp,
// kind-dot ordering, config-file discovery, resource path resolution and
// table grouping. Kept pure so it unit-tests without component plumbing.

/** Explorer drag-resize clamp (T1.3 spec: ~200–420px). */
export const EXPLORER_MIN_WIDTH = 200
export const EXPLORER_MAX_WIDTH = 420

export function clampExplorerWidth(px: number): number {
  return Math.max(EXPLORER_MIN_WIDTH, Math.min(EXPLORER_MAX_WIDTH, Math.round(px)))
}

/** Canonical action-kind order for the flowgroup kind-dots (§4 kind tokens). */
export const KIND_ORDER = ['load', 'transform', 'write', 'test'] as const

/** Kind-dot fill class — the same `--kind-*` tokens the DAG / table badges use. */
export const KIND_DOT_CLASS: Record<string, string> = {
  load: 'bg-kind-load',
  transform: 'bg-kind-transform',
  write: 'bg-kind-write',
  test: 'bg-kind-test',
}

/**
 * Distinct action kinds for a flowgroup summary's `action_types`, canonical
 * kinds first (load, transform, write, test), any unknown kind appended
 * sorted (rendered as a neutral dot by the caller).
 */
export function orderedKinds(actionTypes: readonly string[]): string[] {
  const present = new Set(actionTypes)
  const known = KIND_ORDER.filter((k) => present.has(k))
  const extra = [...present]
    .filter((k) => !(KIND_ORDER as readonly string[]).includes(k))
    .sort()
  return [...known, ...extra]
}

/** Flatten the recursive files tree into every file path (depth-first). */
export function flattenFilePaths(root: FileNode | undefined): string[] {
  if (!root) return []
  const out: string[] = []
  const walk = (n: FileNode): void => {
    if (n.type === 'file') {
      out.push(n.path)
      return
    }
    for (const c of n.children ?? []) walk(c)
  }
  walk(root)
  return out
}

function isYamlPath(p: string): boolean {
  return p.endsWith('.yaml') || p.endsWith('.yml')
}

export interface ConfigFileGroups {
  pipeline: string[]
  job: string[]
  other: string[]
}

/**
 * Partition `config/*.yaml` files by their preferred kind — mirrors the
 * retired ConfigurationPage discovery (`pipeline_config*` → pipeline,
 * `job_config*` / `monitoring_job_config*` → job; any other config/ YAML →
 * `other`). Re-implemented here (not imported from components/config) so the
 * eager shell never pulls the lazy Config chunk — same discipline as
 * CommandBar.treeHasFile.
 */
export function configFilesByKind(paths: readonly string[]): ConfigFileGroups {
  const groups: ConfigFileGroups = { pipeline: [], job: [], other: [] }
  for (const p of paths) {
    if (!p.startsWith('config/') || !isYamlPath(p)) continue
    const name = p.split('/').pop() ?? p
    if (name.startsWith('pipeline_config')) groups.pipeline.push(p)
    else if (name.startsWith('job_config') || name.startsWith('monitoring_job_config'))
      groups.job.push(p)
    else groups.other.push(p)
  }
  const byName = (a: string, b: string): number =>
    (a.split('/').pop() ?? a).localeCompare(b.split('/').pop() ?? b)
  groups.pipeline.sort(byName)
  groups.job.sort(byName)
  groups.other.sort(byName)
  return groups
}

/**
 * Best-effort project-relative path for a named resource: the tree file under
 * `<dir>/` whose basename stem equals `name`, else the convention
 * `<dir>/<name>.yaml`. The resource NAME is the declared name, which usually —
 * but not always — matches the filename; in Wave-1 this path is only the tab
 * identity (the ResourceTab center view keys off the name).
 */
export function resolveResourceFilePath(
  paths: readonly string[],
  dir: string,
  name: string,
): string {
  const prefix = `${dir}/`
  const match = paths.find((p) => {
    if (!p.startsWith(prefix) || !isYamlPath(p)) return false
    const base = p.split('/').pop() ?? p
    return base.replace(/\.(ya?ml)$/i, '') === name
  })
  return match ?? `${dir}/${name}.yaml`
}

// ── Tables lens grouping ─────────────────────────────────────

/** A dataset FQN is a sink when it is namespaced `sink:<type>/<id>`. */
export function isSinkFqn(fullName: string): boolean {
  return fullName.startsWith('sink:')
}

/** Group label for a table FQN — the `catalog.schema` prefix, `'sinks'` for
 * sink datasets, or the whole name when it has no dotted prefix. */
export function tableGroupLabel(fullName: string): string {
  if (isSinkFqn(fullName)) return 'sinks'
  const lastDot = fullName.lastIndexOf('.')
  return lastDot === -1 ? fullName : fullName.slice(0, lastDot)
}

/** Short leaf label for a table FQN (segment after the last dot; sink id for
 * sinks). */
export function tableLeafName(fullName: string): string {
  if (isSinkFqn(fullName)) return fullName.slice('sink:'.length)
  const lastDot = fullName.lastIndexOf('.')
  return lastDot === -1 ? fullName : fullName.slice(lastDot + 1)
}

export interface TableGroup {
  label: string
  tables: TableSummary[]
}

/**
 * Filter (name / flowgroup / pipeline substring) then group tables by schema
 * prefix. Groups and rows both alphabetical; the `sinks` group sorts last.
 */
export function groupTables(tables: readonly TableSummary[], query: string): TableGroup[] {
  const q = query.trim().toLowerCase()
  const filtered = q
    ? tables.filter(
        (t) =>
          t.full_name.toLowerCase().includes(q) ||
          t.flowgroup.toLowerCase().includes(q) ||
          t.pipeline.toLowerCase().includes(q),
      )
    : tables
  const byLabel = new Map<string, TableSummary[]>()
  for (const t of filtered) {
    const label = tableGroupLabel(t.full_name)
    const arr = byLabel.get(label)
    if (arr) arr.push(t)
    else byLabel.set(label, [t])
  }
  const labels = [...byLabel.keys()].sort((a, b) => {
    if (a === 'sinks' && b !== 'sinks') return 1
    if (b === 'sinks' && a !== 'sinks') return -1
    return a.localeCompare(b)
  })
  return labels.map((label) => ({
    label,
    tables: [...byLabel.get(label)!].sort((a, b) =>
      tableLeafName(a.full_name).localeCompare(tableLeafName(b.full_name)),
    ),
  }))
}
