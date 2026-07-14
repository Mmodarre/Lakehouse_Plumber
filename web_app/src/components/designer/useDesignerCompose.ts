import { useCallback, useMemo, useState } from 'react'
import {
  addAction,
  appendActionSource,
  deleteAction,
  duplicateAction,
} from '@/lib/flowgroup-doc'
import type { ActionKind, ActionRead, FlowgroupMeta } from '@/lib/flowgroup-doc'
import type { GraphNode } from '@/types/api'
import { buildInsertion, sourceFieldShape } from './actionSkeleton'
import type { DesignerMutator } from './formModel'

// ── useDesignerCompose — the graph view's authoring behaviour ──
//
// Owns the add-action palette + every graph mutation the canvas triggers:
// insert (append or pre-wired downstream), fan-in, duplicate, delete. Each
// mutation goes through the caller's `commit` (documentStore.mutate via
// useFlowgroupDoc), which live-syncs the edit into the buffer and returns false
// when the document is degraded / not editable.

/** The current canvas selection GraphView exposes to the compose engine and
 * its structural toolbar. Narrowed to the two modes the graph produces:
 * `source` (an editable action, addressed by its canvas node id — a valid
 * mutator address even for duplicate / unnamed names) and `external` (a source
 * produced outside this flowgroup). */
export type InspectorSelection =
  | { mode: 'source'; action: ActionRead; actionId: string }
  | { mode: 'external'; label: string; consumers: string[] }

/** The slice of a flowgroup document the compose engine reads (a narrow view
 * over useFlowgroupDoc — no dependency on the retired designer data layer). */
export interface ComposeDoc {
  found: boolean
  meta: FlowgroupMeta | null
  actions: readonly ActionRead[]
}

export interface DesignerCompose {
  /** Structurally authorable (found, source view, not a template flowgroup). */
  composable: boolean
  /** Authorable AND writable now (not blocked by the dirty guard). */
  canCompose: boolean
  palette: { afterNodeId?: string } | null
  paletteTitle: string
  paletteSubtitle?: string
  /** Open the palette; pass a node id to add downstream of it. */
  openPalette: (afterNodeId?: string) => void
  closePalette: () => void
  /** Node "+" affordance target. */
  handleAddDownstream: (nodeId: string) => void
  handlePick: (kind: ActionKind, subType: string) => void
  /** Producer views the selected action can fan-in (excludes self + current). */
  producerViews: string[]
  /** The selected action reads a string/list source (fan-in applies). */
  canAddInput: boolean
  handleDuplicate: () => void
  handleDelete: () => void
  handleAddInput: (view: string) => void
}

export interface UseDesignerComposeArgs {
  doc: ComposeDoc
  graphNodes: GraphNode[]
  /** Apply an atomic flowgroup-doc mutation (documentStore.mutate). */
  commit: (mutator: DesignerMutator) => boolean
  selection: InspectorSelection | null
  resolved: boolean
  readOnly: boolean
  onSelect: (id: string | null) => void
}

export function useDesignerCompose({
  doc,
  graphNodes,
  commit,
  selection,
  resolved,
  readOnly,
  onSelect,
}: UseDesignerComposeArgs): DesignerCompose {
  const [palette, setPalette] = useState<{ afterNodeId?: string } | null>(null)

  const composable = doc.found && !resolved && doc.meta?.use_template === undefined
  const canCompose = composable && !readOnly

  const upstreamViewOf = useCallback(
    (nodeId: string): string | undefined => {
      const node = graphNodes.find((n) => n.id === nodeId)
      const index = node?.metadata?.actionIndex
      if (typeof index !== 'number') return undefined
      const action = doc.actions[index]
      if (action === undefined) return undefined
      return (action.kind === 'load' || action.kind === 'transform') &&
        typeof action.target === 'string' &&
        action.target !== ''
        ? action.target
        : undefined
    },
    [graphNodes, doc.actions],
  )

  const openPalette = useCallback((afterNodeId?: string) => setPalette({ afterNodeId }), [])
  const closePalette = useCallback(() => setPalette(null), [])
  const handleAddDownstream = useCallback(
    (nodeId: string) => setPalette({ afterNodeId: nodeId }),
    [],
  )

  const handlePick = useCallback(
    (kind: ActionKind, subType: string) => {
      const existing = doc.actions.map((a) => a.name)
      const afterNodeId = palette?.afterNodeId
      const upstreamView = afterNodeId ? upstreamViewOf(afterNodeId) : undefined
      const action = buildInsertion(kind, subType, { existing, upstreamView })
      commit((d) => addAction(d, action, afterNodeId))
      setPalette(null)
      onSelect(action.name as string)
    },
    [doc.actions, palette, upstreamViewOf, commit, onSelect],
  )

  const sourceSel = selection?.mode === 'source' ? selection : null

  const producerViews = useMemo(() => {
    if (sourceSel === null) return []
    const current = new Set(sourceSel.action.sources)
    const selfTarget = sourceSel.action.target
    const views: string[] = []
    for (const a of doc.actions) {
      if ((a.kind !== 'load' && a.kind !== 'transform') || typeof a.target !== 'string') continue
      if (a.target === '' || a.target === selfTarget || current.has(a.target)) continue
      if (!views.includes(a.target)) views.push(a.target)
    }
    return views
  }, [sourceSel, doc.actions])

  const canAddInput =
    sourceSel !== null &&
    sourceSel.action.kind !== undefined &&
    sourceFieldShape(sourceSel.action.kind, sourceSel.action.subType) !== null

  const handleDuplicate = useCallback(() => {
    if (sourceSel === null) return
    commit((d) => {
      duplicateAction(d, sourceSel.actionId)
    })
  }, [sourceSel, commit])

  const handleDelete = useCallback(() => {
    if (sourceSel === null) return
    commit((d) => deleteAction(d, sourceSel.actionId))
    onSelect(null)
  }, [sourceSel, commit, onSelect])

  const handleAddInput = useCallback(
    (view: string) => {
      if (sourceSel === null) return
      commit((d) => appendActionSource(d, sourceSel.actionId, view))
    },
    [sourceSel, commit],
  )

  const afterLabel = palette?.afterNodeId
    ? (graphNodes.find((n) => n.id === palette.afterNodeId)?.label ?? 'action')
    : undefined
  const upstream = palette?.afterNodeId ? upstreamViewOf(palette.afterNodeId) : undefined

  return {
    composable,
    canCompose,
    palette,
    paletteTitle: afterLabel ? `Add downstream of ${afterLabel}` : 'Add action',
    paletteSubtitle: upstream ? `New action reads ${upstream}` : undefined,
    openPalette,
    closePalette,
    handleAddDownstream,
    handlePick,
    producerViews,
    canAddInput,
    handleDuplicate,
    handleDelete,
    handleAddInput,
  }
}
