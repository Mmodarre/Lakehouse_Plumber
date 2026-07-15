import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import {
  Background,
  BackgroundVariant,
  Controls,
  Panel,
  ReactFlow,
  ReactFlowProvider,
  useReactFlow,
  type EdgeTypes,
  type Node,
  type NodeTypes,
} from '@xyflow/react'
import { FileWarning, LayoutTemplate, Plus, TriangleAlert, Waypoints } from 'lucide-react'
import type { LucideIcon } from 'lucide-react'

import { cn } from '@/lib/utils'
import { Button } from '@/components/ui/button'
import { Dialog, DialogContent, DialogHeader, DialogTitle } from '@/components/ui/dialog'
import { EmptyState, type EmptyStateAction } from '@/components/common/EmptyState'
import { LoadingSpinner } from '@/components/common/LoadingSpinner'
import { useWorkspaceStore, type DocKind } from '@/store/workspaceStore'
import { useSelectionStore } from '@/store/selectionStore'
import { ExternalNode } from '@/components/graph/nodes/ExternalNode'
import { EdgeMarkerDefs } from '@/components/graph/edges/DependencyEdge'
import { useElkLayout } from '@/components/graph/useElkLayout'
import { ActionModalEditor } from '@/components/designer/ActionModalEditor'
import { ActionPalette } from '@/components/designer/ActionPalette'
import { DesignerActionNode } from '@/components/designer/DesignerActionNode'
import { DesignerEdge } from '@/components/designer/DesignerEdge'
import { NodeActionsToolbar } from '@/components/designer/NodeActionsToolbar'
import {
  useDesignerCompose,
  type InspectorSelection,
} from '@/components/designer/useDesignerCompose'
import { useDesignerCanvasWiring } from '@/components/designer/useDesignerCanvasWiring'
import {
  EMPTY_DESIGNER_GRAPH,
  toDesignerGraph,
  type DesignerGraphData,
} from '@/components/designer/designerGraph'
import { useFlowgroupDoc } from './useFlowgroupDoc'

// ── GraphView — the "Graph" view of a flowgroup entity tab (§6.3) ─
//
// A recomposition of the old designer/DesignerCanvas onto the unified
// document core. It keeps the canvas machinery verbatim — @xyflow/react +
// useElkLayout, toDesignerGraph wire mapping, DesignerActionNode/ExternalNode,
// useDesignerCanvasWiring (keyboard nav + named-pipe edges), and
// useDesignerCompose (add / duplicate / delete / fan-in) — but ALL reads come
// from useFlowgroupDoc (memoized on `version`) and ALL writes go through its
// atomic `commit` (documentStore.mutate, live-synced into the buffer,
// comment-preserving). The old per-edit re-parse+PUT write path is gone.
//
// Per D2 the graph edits STRUCTURE (add/remove/wire actions); FIELD edits are
// delegated elsewhere: a node click publishes the resolved selection to
// selectionStore (keyed by this tab's id) for the right-dock Inspector's Action
// tab, and a double-click (or the node pencil) opens the staged-save
// ActionModalEditor in a Dialog. The old slide-in DesignerInspector and the
// retired Form view are NOT rendered.
//
// readOnly is useFlowgroupDoc.readOnly only (degraded ∨ viewer ∨ loading): the
// old bufferDirty hard-block (the retired designer canvas's dirty guard) DIES —
// live-sync means an in-flight text edit no longer freezes the graph. Degraded
// parse keeps the last-good projection dimmed under a jump-to-YAML banner (same
// pattern FlowgroupFormView uses).

/** Props contract — identical to FlowgroupFormView (identity-only; the view
 * re-derives everything from the entity document at `filePath`). */
export interface GraphViewProps {
  /** Strip-wide tab id (used to switch this tab to the Form / YAML view). */
  tabId: string
  /** Project-relative path of the flowgroup/template YAML. */
  filePath: string
  docKind: DocKind
}

const nodeTypes: NodeTypes = {
  load: DesignerActionNode,
  transform: DesignerActionNode,
  write: DesignerActionNode,
  test: DesignerActionNode,
  action: DesignerActionNode,
  external: ExternalNode,
}

const edgeTypes: EdgeTypes = {
  dependency: DesignerEdge,
}

const NO_WARNINGS: readonly string[] = []

/** Centered in-canvas state (loading / parse / empty). */
function CanvasNotice({
  icon,
  title,
  message,
  action,
  children,
}: {
  icon: LucideIcon
  title: string
  message: string
  action?: EmptyStateAction
  children?: React.ReactNode
}) {
  return (
    <div className="flex h-full flex-col items-center justify-center overflow-auto">
      <EmptyState icon={icon} title={title} message={message} action={action} />
      {children}
    </div>
  )
}

export function GraphView(props: GraphViewProps) {
  return (
    <ReactFlowProvider>
      <GraphViewInner {...props} />
    </ReactFlowProvider>
  )
}

function GraphViewInner({ tabId, filePath, docKind }: GraphViewProps) {
  const { doc, meta, actions, graph, commit, readOnly, readOnlyReason } = useFlowgroupDoc(
    filePath,
    docKind,
  )
  const { fitView } = useReactFlow()

  const [selectedId, setSelectedId] = useState<string | null>(null)
  // Double-click opens the field editor in a modal (Fix #3); single-click still
  // only selects (ring + structural toolbar). `null` = closed.
  const [modalNodeId, setModalNodeId] = useState<string | null>(null)
  // useDesignerCanvasWiring's Enter handler focuses this ref. The Form view is
  // gone (selection now drives the Inspector's Action tab), so the ref stays a
  // never-focused sink kept only to satisfy the hook's signature.
  const inspectorRef = useRef<HTMLElement | null>(null)

  const degraded = readOnlyReason === 'parse-error'
  const isTemplate = docKind === 'template'
  const pipeline = meta?.pipeline ?? ''
  const flowgroupName = doc?.info.name ?? ''

  const graphData: DesignerGraphData = useMemo(
    () => (graph ? toDesignerGraph(graph, pipeline, flowgroupName) : EMPTY_DESIGNER_GRAPH),
    [graph, pipeline, flowgroupName],
  )

  const { nodes, edges, isLayouting } = useElkLayout(graphData.nodes, graphData.edges)

  const { displayEdges, canvasProps } = useDesignerCanvasWiring({
    rfNodes: nodes,
    rfEdges: edges,
    edgeMeta: graphData.edgeMeta,
    selectedId,
    setSelectedId,
    inspectorRef,
    isLayouting,
    fitView,
  })

  // Selection drives the canvas ring + the structural toolbar. Source view
  // only (no resolved view here — see report), so it is 'source' or 'external'.
  const selection: InspectorSelection | null = useMemo(() => {
    if (selectedId === null) return null
    if (selectedId.startsWith('ext:')) {
      if (graph === null) return null
      const external = graph.externals.find((x) => x.id === selectedId)
      if (external === undefined) return null
      const consumers = graph.edges.filter((e) => e.from === selectedId).map((e) => e.to)
      return { mode: 'external', label: external.label, consumers }
    }
    const node = graphData.nodes.find((n) => n.id === selectedId)
    const index = node?.metadata?.actionIndex
    if (typeof index !== 'number') return null
    const action = actions[index]
    // `selectedId` is the canvas node id — a valid mutator address even for
    // duplicate / unnamed action names (unlike the bare action.name).
    return action === undefined ? null : { mode: 'source', action, actionId: selectedId }
  }, [selectedId, graph, actions, graphData.nodes])

  // Publish the resolved selection to selectionStore so the right-dock
  // Inspector can edit it (node click / compose select → set; pane click →
  // null; tab change / unmount → cleanup clears the previous tab's entry).
  useEffect(() => {
    useSelectionStore.getState().setSelection(tabId, selection)
    return () => {
      useSelectionStore.getState().clearSelection(tabId)
    }
  }, [tabId, selection])

  // useDesignerCompose binds directly to the entity document's narrow surface
  // (found/meta/actions) and its atomic `commit` (documentStore.mutate) — no
  // DesignerDoc / DesignerWrite shims. `commit` returns false when the document
  // is degraded / not editable; the compose engine treats that as a no-op.
  const composeDoc = useMemo(
    () => ({ found: doc !== null, meta, actions }),
    [doc, meta, actions],
  )

  const compose = useDesignerCompose({
    doc: composeDoc,
    graphNodes: graphData.nodes,
    commit,
    selection,
    resolved: false,
    readOnly,
    onSelect: setSelectedId,
  })

  // The double-click gesture AND each node's pencil affordance share one opener
  // → the field-editor modal. External nodes have no editable action.
  const openActionModal = useCallback((nodeId: string) => {
    if (!nodeId.startsWith('ext:')) setModalNodeId(nodeId)
  }, [])

  // The compose "+" callback + selection ring merge into node data.
  const displayNodes = useMemo(
    () =>
      nodes.map((n) => {
        const isExternal = n.id.startsWith('ext:')
        const selected = n.id === selectedId
        const nodeComposable = compose.composable && !isExternal
        // External nodes get no edit / compose affordances.
        if (isExternal) return selected ? { ...n, selected } : n
        return {
          ...n,
          selected,
          data: {
            ...n.data,
            // Pencil affordance keeps field editing discoverable now that the
            // right-dock Action pane is gone — opens the same modal as a
            // double-click (DesignerActionNode renders it from this callback).
            onEditAction: openActionModal,
            ...(nodeComposable
              ? { onAddDownstream: compose.handleAddDownstream, composeReadOnly: readOnly }
              : {}),
          },
        }
      }),
    [nodes, selectedId, compose.composable, compose.handleAddDownstream, readOnly, openActionModal],
  )

  // Degraded / template / no-flowgroup notices jump to the Code view (the old
  // YAML view is now the yaml sub-tab there).
  const jumpToCode = useCallback(() => {
    useWorkspaceStore.getState().setTabView(tabId, 'code')
  }, [tabId])

  const handleNodeClick = useCallback(
    (_: React.MouseEvent, node: Node) => setSelectedId(node.id),
    [],
  )
  const handleNodeDoubleClick = useCallback(
    (_: React.MouseEvent, node: Node) => openActionModal(node.id),
    [openActionModal],
  )
  const handlePaneClick = useCallback(() => setSelectedId(null), [])

  // Resolve the action addressed by the open modal (same canvas-node → action
  // resolution the selection memo uses). `null` keeps the Dialog closed.
  const modalAction = useMemo(() => {
    if (modalNodeId === null || modalNodeId.startsWith('ext:')) return null
    const node = graphData.nodes.find((n) => n.id === modalNodeId)
    const index = node?.metadata?.actionIndex
    if (typeof index !== 'number') return null
    return actions[index] ?? null
  }, [modalNodeId, graphData.nodes, actions])

  const warnings = graph?.warnings ?? NO_WARNINGS

  const flow = (
    <ReactFlow
      nodes={displayNodes}
      edges={displayEdges}
      nodeTypes={nodeTypes}
      edgeTypes={edgeTypes}
      onNodeClick={handleNodeClick}
      onNodeDoubleClick={handleNodeDoubleClick}
      onPaneClick={handlePaneClick}
      nodesDraggable={false}
      nodesConnectable={false}
      // Default true zooms the canvas on every node double-click, which lurches
      // the view and masks the "double-click to edit" gesture. The sibling
      // FlowgroupMiniGraph disables it for the same reason.
      zoomOnDoubleClick={false}
      fitView
      minZoom={0.1}
      maxZoom={2}
      proOptions={{ hideAttribution: true }}
    >
      <EdgeMarkerDefs />
      {/* Dot grid + chrome colors come from the tokened .react-flow CSS block */}
      <Background variant={BackgroundVariant.Dots} gap={20} size={1.2} />
      <Controls showInteractive={false} position="top-right" />
      {compose.canCompose && (
        <Panel position="top-left">
          <Button size="sm" onClick={() => compose.openPalette()}>
            <Plus aria-hidden="true" /> Add action
          </Button>
        </Panel>
      )}
      {selection?.mode === 'source' && (
        <Panel position="bottom-center">
          <div className="overflow-hidden rounded-sm border border-border bg-card shadow-md">
            <NodeActionsToolbar
              readOnly={readOnly}
              producerViews={compose.producerViews}
              canAddInput={compose.canAddInput}
              onDuplicate={compose.handleDuplicate}
              onDelete={compose.handleDelete}
              onAddInput={compose.handleAddInput}
            />
          </div>
        </Panel>
      )}
    </ReactFlow>
  )

  let body: React.ReactNode
  if (readOnlyReason === 'loading') {
    body = <LoadingSpinner className="h-full" />
  } else if (doc === null) {
    body = degraded ? (
      <CanvasNotice
        icon={FileWarning}
        title="This file doesn't parse"
        message="YAML syntax errors — fix them in the Code view."
        action={{ label: 'Open Code view', onClick: jumpToCode, variant: 'outline' }}
      />
    ) : (
      <CanvasNotice
        icon={FileWarning}
        title={isTemplate ? 'No template here' : 'No flowgroup here'}
        message={`This file has no ${
          isTemplate ? 'template' : 'flowgroup'
        } to show as a graph — use the Code view.`}
        action={{ label: 'Open Code view', onClick: jumpToCode, variant: 'outline' }}
      />
    )
  } else if (actions.length === 0 && meta?.use_template !== undefined) {
    body = (
      <CanvasNotice
        icon={LayoutTemplate}
        title="Template-based flowgroup"
        message={`Actions come from template '${meta.use_template}'. Edit them in the template, or use the Code view.`}
        action={{ label: 'Open Code view', onClick: jumpToCode, variant: 'outline' }}
      />
    )
  } else if (actions.length === 0) {
    body = (
      <CanvasNotice
        icon={Waypoints}
        title="No actions yet"
        message={`Add the first action to start building this ${
          isTemplate ? 'template' : 'flowgroup'
        }.`}
      >
        <Button
          size="sm"
          className="mt-1"
          disabled={!compose.canCompose}
          onClick={() => compose.openPalette()}
        >
          <Plus aria-hidden="true" /> Add first action
        </Button>
      </CanvasNotice>
    )
  } else if (isLayouting) {
    body = <LoadingSpinner className="h-full" />
  } else {
    body = flow
  }

  return (
    <div className="flex h-full min-h-0 flex-1 flex-col">
      {degraded && (
        <div className="flex items-center gap-2 border-b border-warning/30 bg-warning/10 px-4 py-2 text-xs text-foreground">
          <TriangleAlert className="size-3.5 shrink-0 text-warning" aria-hidden="true" />
          <span className="flex-1">YAML has syntax errors — fix in the Code view.</span>
          <Button type="button" variant="outline" size="sm" onClick={jumpToCode}>
            Open Code view
          </Button>
        </div>
      )}
      {!degraded && warnings.length > 0 && (
        <div
          className="flex items-start gap-2 border-b border-warning/25 bg-warning/12 px-4 py-1.5 text-xs text-foreground"
          title={warnings.join('\n')}
        >
          <TriangleAlert className="mt-0.5 size-3.5 shrink-0 text-warning" aria-hidden="true" />
          <span className="truncate">
            {warnings[0]}
            {warnings.length > 1 && ` (+${warnings.length - 1} more)`}
          </span>
        </div>
      )}
      <div
        className={cn(
          'relative min-w-0 flex-1 outline-none focus-visible:ring-2 focus-visible:ring-inset focus-visible:ring-ring/60',
          // Degraded: dim the last-good projection; the banner stays clickable.
          degraded && 'pointer-events-none opacity-60',
        )}
        {...canvasProps}
      >
        {body}
      </div>
      <ActionPalette
        open={compose.palette !== null}
        title={compose.paletteTitle}
        subtitle={compose.paletteSubtitle}
        onClose={compose.closePalette}
        onPick={compose.handlePick}
      />
      <Dialog
        open={modalAction !== null}
        onOpenChange={(open) => {
          if (!open) setModalNodeId(null)
        }}
      >
        <DialogContent className="max-h-[85vh] gap-0 overflow-y-auto p-0 sm:max-w-3xl">
          <DialogHeader className="border-b border-border px-4 py-3">
            <DialogTitle className="text-sm">Edit action</DialogTitle>
          </DialogHeader>
          {modalAction !== null && modalNodeId !== null && (
            <ActionModalEditor
              // Fresh working handle per opened node. The staged-save modal
              // holds edits on a detached handle and only replays them into the
              // shared buffer + persists to disk on Save; onSaved fires on a
              // clean persist → close the modal.
              key={`${filePath}:${modalNodeId}`}
              action={modalAction}
              actionId={modalNodeId}
              filePath={filePath}
              docKind={docKind}
              onSaved={() => setModalNodeId(null)}
              // Cancel discards the staged edits and closes — the same close as
              // the Dialog's X / Escape (both null out modalNodeId).
              onCancel={() => setModalNodeId(null)}
              // Failed persist (412 / yaml_error): close the modal and jump to
              // the Code view where the ConflictDialog / syntax fix lives (Fix
              // #2) — the staged edits are dropped with the modal.
              onOpenCodeView={() => {
                setModalNodeId(null)
                jumpToCode()
              }}
            />
          )}
        </DialogContent>
      </Dialog>
    </div>
  )
}
