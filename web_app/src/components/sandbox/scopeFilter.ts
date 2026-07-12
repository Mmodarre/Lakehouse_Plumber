// ── Sandbox scope filters — pure, testable ──────────────────
//
// When developer-sandbox mode is ON, the IDE narrows what it shows to the
// pipelines resolved from `.lhp/profile.yaml` (GET /api/sandbox →
// `resolved_pipelines`). These filters do that narrowing and NOTHING else:
// every one takes a `scope` of `Set<string> | null` where `null` means "not
// scoping" and returns its input unchanged (identity). Components read the
// scope once (via `resolveScope` + the query) and hand it here, so the
// branching lives in one tested place instead of scattered through the UI.

import type {
  FileNode,
  FlowgroupSummary,
  GraphEdge,
  GraphNode,
  SandboxScope,
} from '../../types/api'

/** The concrete scope to filter by, or `null` to disable all filtering. */
export type Scope = ReadonlySet<string> | null

/**
 * Derive the active pipeline scope from the toggle + the GET /api/sandbox
 * view. Returns `null` (⇒ every filter is identity) unless sandbox is ON,
 * a profile exists, the scope resolved without error, and it is non-empty.
 *
 * A zero-match glob or a malformed profile arrives as `error` with an empty
 * `resolved_pipelines`; scoping to nothing would hide the whole project, so
 * those degrade to `null` (no filtering) — the failure is surfaced in the
 * pill/picker instead of blanking the IDE.
 */
export function resolveScope(
  enabled: boolean,
  data: SandboxScope | undefined,
): Scope {
  if (!enabled || !data || !data.profile_exists || data.error) return null
  const resolved = data.resolved_pipelines ?? []
  return resolved.length > 0 ? new Set(resolved) : null
}

/**
 * Filter a pipeline-level dependency graph to the scope.
 *
 * External-source nodes are always kept (a scoped pipeline may still draw
 * from an out-of-scope external table). Every other node is kept only when
 * its `pipeline` is in scope. Edges are then kept only when BOTH endpoints
 * survive, so an edge into a dropped pipeline is dropped with it — no
 * dangling references. An external node left with no surviving edge simply
 * becomes disconnected; the canvas already collapses disconnected externals
 * into its "External sources" band.
 */
export function filterGraphForScope(
  nodes: readonly GraphNode[],
  edges: readonly GraphEdge[],
  scope: Scope,
): { nodes: GraphNode[]; edges: GraphEdge[] } {
  if (scope === null) return { nodes: [...nodes], edges: [...edges] }

  const keptNodes = nodes.filter(
    (n) => n.type === 'external' || scope.has(n.pipeline),
  )
  const keptIds = new Set(keptNodes.map((n) => n.id))
  const keptEdges = edges.filter(
    (e) => keptIds.has(e.source) && keptIds.has(e.target),
  )
  return { nodes: keptNodes, edges: keptEdges }
}

/** Keep only flowgroups whose pipeline is in scope. */
export function filterFlowgroupsForScope(
  flowgroups: readonly FlowgroupSummary[],
  scope: Scope,
): FlowgroupSummary[] {
  if (scope === null) return [...flowgroups]
  return flowgroups.filter((fg) => scope.has(fg.pipeline))
}

/**
 * Build the `source_file → pipeline` map the file-tree filter keys off.
 *
 * Flowgroup sources live under `pipelines/`; every other project file
 * (`lhp.yaml`, `substitutions/`, `presets/`, `templates/`, `blueprints/`, …)
 * is absent from this map and is therefore never hidden. Blank source paths
 * (unknown / outside the project root) are skipped so they stay visible.
 */
export function buildSourceFileToPipeline(
  flowgroups: readonly FlowgroupSummary[],
): Map<string, string> {
  const map = new Map<string, string>()
  for (const fg of flowgroups) {
    if (fg.source_file) map.set(fg.source_file, fg.pipeline)
  }
  return map
}

/**
 * Hide out-of-scope pipeline files from the project file tree.
 *
 * A file is hidden only when it maps (via `sourceToPipeline`) to a pipeline
 * that is NOT in scope. A file with no mapping is kept — shared config, and
 * any `pipelines/` file we can't classify, stay visible (be conservative:
 * never hide what we can't attribute). A directory is dropped only when it
 * had children and filtering removed all of them; an already-empty directory
 * is left untouched. The returned tree is a fresh structure — the input is
 * never mutated.
 */
export function filterFileTreeForScope(
  root: FileNode,
  sourceToPipeline: ReadonlyMap<string, string>,
  scope: Scope,
): FileNode {
  if (scope === null) return root
  return pruneNode(root, sourceToPipeline, scope) ?? { ...root, children: [] }
}

/** Return the filtered node, or `null` when it should be hidden entirely. */
function pruneNode(
  node: FileNode,
  sourceToPipeline: ReadonlyMap<string, string>,
  scope: ReadonlySet<string>,
): FileNode | null {
  if (node.type === 'file') {
    const owner = sourceToPipeline.get(node.path)
    // Unclassifiable (no owner) files stay; classified files need an in-scope owner.
    if (owner !== undefined && !scope.has(owner)) return null
    return node
  }

  const children = node.children ?? []
  const kept = children
    .map((c) => pruneNode(c, sourceToPipeline, scope))
    .filter((c): c is FileNode => c !== null)

  // Drop a directory only when it emptied out because of filtering.
  if (children.length > 0 && kept.length === 0) return null
  return { ...node, children: kept }
}
