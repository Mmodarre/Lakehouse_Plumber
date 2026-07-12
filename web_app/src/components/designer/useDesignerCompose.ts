import { useCallback, useMemo, useState } from 'react'
import {
  addAction,
  appendActionSource,
  deleteAction,
  duplicateAction,
} from '@/lib/flowgroup-doc'
import type { ActionKind } from '@/lib/flowgroup-doc'
import type { GraphNode } from '@/types/api'
import { buildInsertion, sourceFieldShape } from './actionSkeleton'
import type { DesignerDoc } from './useDesignerDoc'
import type { DesignerWrite } from './useDesignerWrite'
import type { InspectorSelection } from './DesignerInspector'

// ── useDesignerCompose — the designer's authoring behaviour ──
//
// Owns the add-action palette + every graph mutation the canvas triggers:
// insert (append or pre-wired downstream), fan-in, duplicate, delete. Each
// mutation goes through `write.commit`, which is a hard no-op while the file
// buffer is dirty — so the enforcing dirty-guard covers composition for free.

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
  doc: DesignerDoc
  graphNodes: GraphNode[]
  write: DesignerWrite
  selection: InspectorSelection | null
  resolved: boolean
  readOnly: boolean
  onSelect: (id: string | null) => void
}

export function useDesignerCompose({
  doc,
  graphNodes,
  write,
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
      write.commit((d) => addAction(d, action, afterNodeId))
      setPalette(null)
      onSelect(action.name as string)
    },
    [doc.actions, palette, upstreamViewOf, write, onSelect],
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
    write.commit((d) => {
      duplicateAction(d, sourceSel.actionId)
    })
  }, [sourceSel, write])

  const handleDelete = useCallback(() => {
    if (sourceSel === null) return
    write.commit((d) => deleteAction(d, sourceSel.actionId))
    onSelect(null)
  }, [sourceSel, write, onSelect])

  const handleAddInput = useCallback(
    (view: string) => {
      if (sourceSel === null) return
      write.commit((d) => appendActionSource(d, sourceSel.actionId, view))
    },
    [sourceSel, write],
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
