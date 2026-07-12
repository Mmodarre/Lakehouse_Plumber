import type { Edge } from '@xyflow/react'
import type { ActionKind, FlowgroupGraph } from '@/lib/flowgroup-doc'
import type { FlowgroupActionSummary, GraphEdge, GraphNode } from '@/types/api'

// ── designerGraph — pure canvas data mapping ─────────────────
//
// Maps the two designer data sources — the client-side document model
// (lib/flowgroup-doc deriveGraph) and the resolved-flowgroup DTO — onto the
// shared graph-rendering contract (types/api GraphNode/GraphEdge) that
// useElkLayout consumes. Pure TypeScript: no React, no I/O, fully testable.

/**
 * Designer-only edge facts the wire-shaped GraphEdge cannot carry: the view
 * name that induced the edge (reserved for the later named-pipe edge
 * treatment) and the semantic kind. Index-aligned with `edges`.
 */
export interface DesignerEdgeMeta {
  viewName: string
  kind: 'data' | 'depends_on'
}

export interface DesignerGraphData {
  nodes: GraphNode[]
  edges: GraphEdge[]
  /** `edgeMeta[i]` belongs to `edges[i]` (useElkLayout preserves edge order). */
  edgeMeta: DesignerEdgeMeta[]
}

/** Stable empty value so layout inputs keep referential identity. */
export const EMPTY_DESIGNER_GRAPH: DesignerGraphData = {
  nodes: [],
  edges: [],
  edgeMeta: [],
}

const WRITE_TYPE_LABELS: Record<string, string> = {
  streaming_table: 'Streaming table',
  materialized_view: 'Materialized view',
  sink: 'Sink',
}

function capitalize(text: string): string {
  return text.length === 0 ? text : text.charAt(0).toUpperCase() + text.slice(1)
}

/** Node sublabel for the source view (sub-types from the YAML + CLI defaults). */
export function sourceSublabel(
  kind: ActionKind | 'unknown',
  subType: string,
  writeMode?: string,
): string {
  switch (kind) {
    case 'load':
      return `Load (${subType})`
    case 'transform':
      return `Transform (${subType})`
    case 'write': {
      const base = WRITE_TYPE_LABELS[subType] ?? capitalize(subType)
      return writeMode !== undefined && writeMode !== 'standard'
        ? `${base} · ${writeMode}`
        : base
    }
    case 'test':
      return `Test (${subType})`
    default:
      return 'Unknown type'
  }
}

/**
 * Node sublabel for the resolved view. The resolved DTO does not expose
 * `write_target.type`, so write nodes deliberately say "Write" (plus
 * mode/SCD) instead of claiming Streaming table vs Materialized view.
 */
export function resolvedSublabel(action: FlowgroupActionSummary): string {
  switch (action.type) {
    case 'load':
      return 'Load'
    case 'transform':
      return action.transform_type ? `Transform (${action.transform_type})` : 'Transform'
    case 'write': {
      const parts = ['Write']
      if (action.write_mode) parts.push(action.write_mode)
      if (action.scd_type != null) parts.push(`SCD ${action.scd_type}`)
      return parts.join(' · ')
    }
    case 'test':
      return action.test_type ? `Test (${action.test_type})` : 'Test'
    default:
      return capitalize(action.type)
  }
}

const ACTION_NODE_TYPES: readonly string[] = ['load', 'transform', 'write', 'test']

/**
 * Source view: flowgroup-doc's derived graph → layout inputs. Action nodes
 * keep their kind as the node type ('action' for unknown kinds); externals
 * render as the shared muted ExternalNode. Edge visual classes reuse the
 * shared DependencyEdge styles: inputs from outside the flowgroup render
 * dashed+faded ('external'), manual `depends_on` edges render dashed
 * ('cross_flowgroup' is a style key here, not a semantic claim), and data
 * edges render solid ('internal').
 */
export function toDesignerGraph(
  graph: FlowgroupGraph,
  pipeline: string,
  flowgroup: string,
): DesignerGraphData {
  const nodes: GraphNode[] = graph.nodes.map((n) => ({
    id: n.id,
    label: n.name !== '' ? n.name : `(action ${n.actionIndex + 1})`,
    type: n.kind === 'unknown' ? 'action' : n.kind,
    pipeline,
    flowgroup,
    stage: 0,
    metadata: {
      sublabel: sourceSublabel(n.kind, n.subType, n.writeMode),
      actionIndex: n.actionIndex,
    },
  }))
  for (const ext of graph.externals) {
    nodes.push({
      id: ext.id,
      label: ext.label,
      type: 'external',
      pipeline,
      flowgroup,
      stage: 0,
      metadata: {},
    })
  }

  const edges: GraphEdge[] = []
  const edgeMeta: DesignerEdgeMeta[] = []
  for (const e of graph.edges) {
    edges.push({
      source: e.from,
      target: e.to,
      type: e.from.startsWith('ext:')
        ? 'external'
        : e.kind === 'depends_on'
          ? 'cross_flowgroup'
          : 'internal',
    })
    edgeMeta.push({ viewName: e.viewName, kind: e.kind })
  }
  return { nodes, edges, edgeMeta }
}

/**
 * Resolved view: the endpoint's slim ActionViews carry no source/dependency
 * fields (`FlowgroupActionSummary` has name/type/target/write metadata only),
 * so no edges can be derived — resolved renders as a laid-out node list and
 * the canvas states that dependencies come from the source view.
 */
export function resolvedDesignerGraph(
  actions: FlowgroupActionSummary[],
  pipeline: string,
  flowgroup: string,
): DesignerGraphData {
  const occurrences = new Map<string, number>()
  const nodes: GraphNode[] = actions.map((action, index) => {
    const base = action.name !== '' ? action.name : `__action_${index}`
    const count = (occurrences.get(base) ?? 0) + 1
    occurrences.set(base, count)
    return {
      id: count === 1 ? base : `${base}#${count}`,
      label: action.name !== '' ? action.name : `(action ${index + 1})`,
      type: ACTION_NODE_TYPES.includes(action.type) ? action.type : 'action',
      pipeline,
      flowgroup,
      stage: 0,
      metadata: {
        sublabel: resolvedSublabel(action),
        actionIndex: index,
      },
    }
  })
  return { nodes, edges: [], edgeMeta: [] }
}

/**
 * Attach the designer edge facts to the React Flow edges produced by
 * useElkLayout (which maps `edges[i]` → rf edge `e-{i}` in order). Returns
 * the input unchanged on a length mismatch (mid-relayout render).
 */
export function attachEdgeMeta(rfEdges: Edge[], meta: DesignerEdgeMeta[]): Edge[] {
  if (rfEdges.length !== meta.length) return rfEdges
  return rfEdges.map((edge, i) => ({
    ...edge,
    data: { ...edge.data, viewName: meta[i].viewName, designerKind: meta[i].kind },
  }))
}

// ── Inspector: key-config derivation ─────────────────────────

export interface ConfigRow {
  /** Dotted key path within the action mapping (e.g. `source.path`). */
  key: string
  value: string
}

/** Top-level action fields the inspector already shows as fixed rows. */
const SKIP_TOP_LEVEL = new Set(['name', 'type', 'target', 'description', 'depends_on'])

const MAX_SCALAR_CHARS = 80
const MAX_LIST_ITEMS = 3
const MAX_FLATTEN_DEPTH = 2

function formatScalar(value: string | number | boolean): string {
  const text = String(value)
  return text.length > MAX_SCALAR_CHARS ? `${text.slice(0, MAX_SCALAR_CHARS - 1)}…` : text
}

function isScalar(value: unknown): value is string | number | boolean {
  return (
    typeof value === 'string' || typeof value === 'number' || typeof value === 'boolean'
  )
}

function summarizeArray(values: unknown[]): string {
  if (values.every(isScalar)) {
    const shown = values.slice(0, MAX_LIST_ITEMS).map(formatScalar).join(', ')
    return values.length > MAX_LIST_ITEMS
      ? `${shown}, … (+${values.length - MAX_LIST_ITEMS})`
      : shown
  }
  return `${values.length} item${values.length === 1 ? '' : 's'}`
}

function walk(rows: ConfigRow[], key: string, value: unknown, depth: number): void {
  if (value === null || value === undefined) return
  if (isScalar(value)) {
    rows.push({ key, value: formatScalar(value) })
    return
  }
  if (Array.isArray(value)) {
    if (value.length > 0) rows.push({ key, value: summarizeArray(value) })
    return
  }
  if (typeof value === 'object') {
    const entries = Object.entries(value as Record<string, unknown>)
    if (depth >= MAX_FLATTEN_DEPTH) {
      rows.push({ key, value: `${entries.length} field${entries.length === 1 ? '' : 's'}` })
      return
    }
    for (const [k, v] of entries) walk(rows, `${key}.${k}`, v, depth + 1)
  }
}

/**
 * Quiet definition-list rows for an action's remaining configuration:
 * top-level scalars, scalar lists (first items), and nested mappings
 * (`source`, `write_target`, …) flattened to dotted keys, two levels deep.
 * YAML key order is preserved. Fixed identity fields are excluded.
 */
export function actionConfigRows(raw: Record<string, unknown>): ConfigRow[] {
  const rows: ConfigRow[] = []
  for (const [key, value] of Object.entries(raw)) {
    if (SKIP_TOP_LEVEL.has(key)) continue
    walk(rows, key, value, 0)
  }
  return rows
}
