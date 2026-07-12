import { useCallback, useMemo, useRef, useState } from 'react'
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
import {
  FileWarning,
  Info,
  LayoutTemplate,
  Package,
  Plus,
  TriangleAlert,
  Waypoints,
} from 'lucide-react'
import { useUIStore } from '@/store/uiStore'
import { useWorkspaceStore } from '@/store/workspaceStore'
import { useFlowgroupResolved } from '@/hooks/useFlowgroups'
import { errorMessage } from '@/lib/errors'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { EmptyState } from '@/components/common/EmptyState'
import type { EmptyStateAction } from '@/components/common/EmptyState'
import { LoadingSpinner } from '@/components/common/LoadingSpinner'
import type { LucideIcon } from 'lucide-react'
import { ExternalNode } from '../graph/nodes/ExternalNode'
import { EdgeMarkerDefs } from '../graph/edges/DependencyEdge'
import { useElkLayout } from '../graph/useElkLayout'
import { ConfigConflictDialog } from '../config/ConfigConflictDialog'
import { loadBufferContent } from '../workspace/flowgroupBuffers'
import { ActionPalette } from './ActionPalette'
import { CodeModal, type CodeTarget } from './CodeModal'
import { DesignerActionNode } from './DesignerActionNode'
import { DesignerEdge } from './DesignerEdge'
import { DesignerInspector, type InspectorSelection } from './DesignerInspector'
import { useDesignerCanvasWiring } from './useDesignerCanvasWiring'
import { DesignerParametersPanel } from './DesignerParametersPanel'
import { DesignerProblemsStrip } from './DesignerProblemsStrip'
import { DesignerTopBar } from './DesignerTopBar'
import { useDesignerCompose } from './useDesignerCompose'
import { useDesignerDoc, type DesignerDocKind } from './useDesignerDoc'
import { useDesignerValidation } from './useDesignerValidation'
import { useDesignerWrite } from './useDesignerWrite'
import type { ValidationNode } from './designerValidation'
import {
  EMPTY_DESIGNER_GRAPH,
  resolvedDesignerGraph,
  toDesignerGraph,
  type DesignerGraphData,
} from './designerGraph'

/** Tab-strip icon category for a companion file opened as a workspace buffer. */
function categoryForPath(path: string): string {
  const ext = path.split('.').pop()?.toLowerCase()
  if (ext === 'sql') return 'sql'
  if (ext === 'py') return 'python'
  if (ext === 'json') return 'expectations'
  if (ext === 'yaml' || ext === 'yml') return 'yaml'
  return 'file'
}

export interface DesignerCanvasProps {
  pipeline: string
  flowgroup: string
  /** Project-relative path of the flowgroup YAML this canvas renders. */
  filePath: string
  /** 'template' opens the file under templates/ in template-authoring mode. */
  docKind?: DesignerDocKind
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

/** Centered in-canvas state (loading failures, parse errors, empty docs). */
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

/**
 * Per-flowgroup designer canvas, lazy-loaded by WorkspaceEditor when a
 * designer tab is active (keyed by tab id). Read path: the canvas is a pure
 * projection of the flowgroup's YAML file — it re-derives everything from
 * `filePath` and never writes.
 */
export default function DesignerCanvas(props: DesignerCanvasProps) {
  return (
    <ReactFlowProvider>
      <DesignerCanvasInner {...props} />
    </ReactFlowProvider>
  )
}

function DesignerCanvasInner({
  pipeline,
  flowgroup,
  filePath,
  docKind = 'flowgroup',
}: DesignerCanvasProps) {
  const isTemplate = docKind === 'template'
  const doc = useDesignerDoc(filePath, flowgroup, docKind)
  const env = useUIStore((s) => s.selectedEnv)
  const bufferDirty = useWorkspaceStore((s) =>
    s.buffers.some((b) => b.path === filePath && b.isDirty),
  )
  // Dirty-buffer guard is enforcing: the write path is hard-blocked (not just
  // UI-disabled) while the file is open as unsaved text.
  const write = useDesignerWrite(filePath, flowgroup, bufferDirty, docKind)
  const readOnly = bufferDirty || write.conflict
  const { fitView } = useReactFlow()

  const [resolved, setResolved] = useState(false)
  const [selectedId, setSelectedId] = useState<string | null>(null)
  const [codeTarget, setCodeTarget] = useState<CodeTarget | null>(null)
  const inspectorRef = useRef<HTMLElement | null>(null)
  const resolvedQuery = useFlowgroupResolved(resolved ? flowgroup : null, env)
  const resolvedActions = resolvedQuery.data?.flowgroup.actions

  const graphData: DesignerGraphData = useMemo(() => {
    if (resolved) {
      return resolvedActions
        ? resolvedDesignerGraph(resolvedActions, pipeline, flowgroup)
        : EMPTY_DESIGNER_GRAPH
    }
    return doc.graph ? toDesignerGraph(doc.graph, pipeline, flowgroup) : EMPTY_DESIGNER_GRAPH
  }, [resolved, resolvedActions, doc.graph, pipeline, flowgroup])

  // Validate: on-demand, scoped to this pipeline, mapped onto the canvas nodes.
  const validationNodes = useMemo<ValidationNode[]>(
    () =>
      graphData.nodes
        .filter((n) => !n.id.startsWith('ext:'))
        .map((n) => ({ id: n.id, name: n.label })),
    [graphData.nodes],
  )
  const validation = useDesignerValidation({
    pipeline,
    flowgroup,
    content: doc.content,
    nodes: validationNodes,
  })

  const { nodes, edges, isLayouting } = useElkLayout(graphData.nodes, graphData.edges)

  // Named-pipe edges, one-shot draw-on motion, and keyboard nav (kept thin).
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

  // Selection drives the canvas ring + inspector; computed before compose.
  const selection: InspectorSelection | null = useMemo(() => {
    if (selectedId === null) return null
    if (selectedId.startsWith('ext:')) {
      if (resolved || doc.graph === null) return null
      const external = doc.graph.externals.find((x) => x.id === selectedId)
      if (external === undefined) return null
      const consumers = doc.graph.edges.filter((e) => e.from === selectedId).map((e) => e.to)
      return { mode: 'external', label: external.label, consumers }
    }
    const node = graphData.nodes.find((n) => n.id === selectedId)
    const index = node?.metadata?.actionIndex
    if (typeof index !== 'number') return null
    if (resolved) {
      const action = resolvedActions?.[index]
      return action === undefined ? null : { mode: 'resolved', action }
    }
    const action = doc.actions[index]
    // `selectedId` is the canvas node id — a valid mutator address even for
    // duplicate or unnamed action names (unlike the bare action.name).
    return action === undefined ? null : { mode: 'source', action, actionId: selectedId }
  }, [selectedId, resolved, doc.graph, doc.actions, graphData.nodes, resolvedActions])

  const compose = useDesignerCompose({
    doc,
    graphNodes: graphData.nodes,
    write,
    selection,
    resolved,
    readOnly,
    onSelect: setSelectedId,
  })

  // Validation counts + the compose "+" callback merge into node data.
  const displayNodes = useMemo(
    () =>
      nodes.map((n) => {
        const counts = validation.perNode.get(n.id)
        const selected = n.id === selectedId
        const nodeComposable = compose.composable && !n.id.startsWith('ext:')
        if (counts === undefined && !selected && !nodeComposable) return n
        return {
          ...n,
          selected,
          data: {
            ...n.data,
            ...(counts === undefined ? {} : { validation: counts }),
            ...(nodeComposable
              ? { onAddDownstream: compose.handleAddDownstream, composeReadOnly: readOnly }
              : {}),
          },
        }
      }),
    [nodes, selectedId, validation.perNode, compose.composable, compose.handleAddDownstream, readOnly],
  )

  const handleNodeClick = useCallback(
    (_: React.MouseEvent, node: Node) => setSelectedId(node.id),
    [],
  )
  const handlePaneClick = useCallback(() => setSelectedId(null), [])

  /** Open a file as a workspace buffer: focus it if already open, else fetch. */
  const openFileBuffer = useCallback((path: string, category: string) => {
    const ws = useWorkspaceStore.getState()
    const alreadyOpen = ws.buffers.some((b) => b.path === path)
    ws.openBuffer(path, { category, exists: true, loading: true })
    if (!alreadyOpen) void loadBufferContent(path)
  }, [])

  /** "Open file as text": focus (or open + fetch) the YAML as a buffer. */
  const openAsText = useCallback(() => openFileBuffer(filePath, 'yaml'), [openFileBuffer, filePath])

  /** Preset badge click: open the preset file as a text buffer. */
  const openPreset = useCallback(
    (name: string) => openFileBuffer(`presets/${name}.yaml`, 'yaml'),
    [openFileBuffer],
  )

  /** "Open as file tab" from the code modal: open the companion file docked. */
  const openCompanionFile = useCallback(
    (path: string) => openFileBuffer(path, categoryForPath(path)),
    [openFileBuffer],
  )

  const warnings = useMemo(
    () => [...doc.fileWarnings, ...(doc.graph?.warnings ?? [])],
    [doc.fileWarnings, doc.graph],
  )

  const flow = (
    <ReactFlow
      nodes={displayNodes}
      edges={displayEdges}
      nodeTypes={nodeTypes}
      edgeTypes={edgeTypes}
      onNodeClick={handleNodeClick}
      onPaneClick={handlePaneClick}
      nodesDraggable={false}
      nodesConnectable={false}
      fitView
      minZoom={0.1}
      maxZoom={2}
      proOptions={{ hideAttribution: true }}
    >
      <EdgeMarkerDefs />
      {/* Dot grid + chrome colors come from the tokened .react-flow CSS block */}
      <Background variant={BackgroundVariant.Dots} gap={20} size={1.2} />
      <Controls showInteractive={false} position="top-right" />
      {resolved && (
        <Panel position="top-left">
          <Badge
            variant="outline"
            className="h-5 rounded-sm bg-card px-1.5 text-2xs text-muted-foreground"
          >
            resolved view · {env}
          </Badge>
        </Panel>
      )}
    </ReactFlow>
  )

  let body: React.ReactNode
  if (resolved) {
    if (resolvedQuery.isPending || isLayouting) {
      body = <LoadingSpinner className="h-full" />
    } else if (resolvedQuery.isError) {
      body = (
        <CanvasNotice
          icon={FileWarning}
          title="Failed to resolve flowgroup"
          message={errorMessage(resolvedQuery.error, `Resolution with env '${env}' failed.`)}
          action={{ label: 'Back to source', onClick: () => setResolved(false), variant: 'outline' }}
        />
      )
    } else if (graphData.nodes.length === 0) {
      body = (
        <CanvasNotice
          icon={Waypoints}
          title="No resolved actions"
          message="Template and preset expansion produced no actions."
        />
      )
    } else {
      body = flow
    }
  } else if (doc.isLoading) {
    body = <LoadingSpinner className="h-full" />
  } else if (doc.loadError !== null) {
    body = (
      <CanvasNotice
        icon={FileWarning}
        title="Failed to load file"
        message={doc.loadError}
        action={{ label: 'Retry', onClick: doc.refetch, variant: 'outline' }}
      />
    )
  } else if (doc.parseError !== null) {
    body = (
      <CanvasNotice
        icon={FileWarning}
        title="This file doesn't parse"
        message={doc.parseError}
        action={{ label: 'Open file as text', onClick: openAsText, variant: 'outline' }}
      />
    )
  } else if (!doc.found) {
    body = doc.blueprintLike ? (
      <CanvasNotice
        icon={Package}
        title="Blueprint file"
        message="This file is a blueprint definition or instance — the designer can't author it yet. The resolved view shows the expanded actions."
        action={{ label: 'View resolved', onClick: () => setResolved(true), variant: 'outline' }}
      />
    ) : (
      <CanvasNotice
        icon={FileWarning}
        title={`Flowgroup '${flowgroup}' not found`}
        message="The file no longer contains a flowgroup with this name."
        action={{ label: 'Open file as text', onClick: openAsText, variant: 'outline' }}
      />
    )
  } else if (doc.actions.length === 0 && doc.meta?.use_template !== undefined) {
    body = (
      <CanvasNotice
        icon={LayoutTemplate}
        title="Template-based flowgroup"
        message={`Actions come from template '${doc.meta.use_template}'. Switch to the resolved view to see them.`}
        action={{ label: 'View resolved', onClick: () => setResolved(true), variant: 'outline' }}
      />
    )
  } else if (doc.actions.length === 0) {
    body = (
      <CanvasNotice
        icon={Waypoints}
        title="No actions yet"
        message={
          readOnly
            ? 'Save or discard the unsaved text edits to start composing here.'
            : `Add the first action to start building this ${isTemplate ? 'template' : 'flowgroup'}.`
        }
      >
        <Button size="sm" className="mt-1" disabled={readOnly} onClick={() => compose.openPalette()}>
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
    <div className="flex min-h-0 flex-1 flex-col">
      <DesignerTopBar
        pipeline={pipeline}
        flowgroup={flowgroup}
        meta={doc.meta}
        isTemplate={isTemplate}
        env={env}
        resolved={resolved}
        onResolvedChange={setResolved}
        validateStatus={validation.status}
        errorCount={validation.errorCount}
        warningCount={validation.warningCount}
        errored={validation.errored}
        canValidate={validation.canRun}
        onValidate={validation.run}
        onAddAction={compose.canCompose ? () => compose.openPalette() : undefined}
      />
      {bufferDirty && (
        <div className="flex items-center gap-2 border-b border-border bg-muted px-4 py-1.5 text-xs text-muted-foreground">
          <Info className="size-3.5 shrink-0" aria-hidden="true" />
          This file has unsaved edits in the text editor — editing is disabled here. Save or
          discard them first.
        </div>
      )}
      {write.yamlError !== null && (
        <div className="flex items-start gap-2 border-b border-destructive/25 bg-destructive/10 px-4 py-1.5 text-xs text-foreground">
          <FileWarning className="mt-0.5 size-3.5 shrink-0 text-destructive" aria-hidden="true" />
          <span className="truncate">
            The last save wrote a YAML syntax error: {write.yamlError.message}
          </span>
        </div>
      )}
      {!resolved && warnings.length > 0 && (
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
      {resolved && resolvedActions !== undefined && (
        <div className="border-b border-border bg-muted px-4 py-1.5 text-xs text-muted-foreground">
          Dependency edges are derived from the source YAML only — the resolved endpoint
          lists actions without their wiring.
        </div>
      )}
      <div className="flex min-h-0 flex-1">
        {isTemplate && (
          <DesignerParametersPanel
            params={doc.templateParams}
            templateName={doc.templateInfo?.name ?? flowgroup}
            commit={write.commit}
            readOnly={readOnly}
          />
        )}
        <div
          className="relative min-w-0 flex-1 outline-none focus-visible:ring-2 focus-visible:ring-inset focus-visible:ring-ring/60"
          {...canvasProps}
        >
          {body}
        </div>
        <DesignerInspector
          panelRef={inspectorRef}
          selection={selection}
          presets={doc.meta?.presets ?? []}
          commit={write.commit}
          rename={write.rename}
          readOnly={readOnly}
          saving={write.saving}
          onRenamed={setSelectedId}
          onOpenPreset={openPreset}
          onEditCode={setCodeTarget}
          producerViews={compose.producerViews}
          canAddInput={compose.canAddInput}
          onDuplicate={compose.handleDuplicate}
          onDelete={compose.handleDelete}
          onAddInput={compose.handleAddInput}
        />
      </div>
      {/* Validation is pipeline-scoped; a template has no pipeline to run it. */}
      {!isTemplate && (
        <DesignerProblemsStrip
          status={validation.status}
          errorCount={validation.errorCount}
          warningCount={validation.warningCount}
          issues={validation.issues}
          errored={validation.errored}
          onSelectNode={setSelectedId}
        />
      )}
      <ConfigConflictDialog
        path={write.conflict ? filePath : null}
        saving={write.saving}
        onReload={() => void write.reload()}
        onOverwrite={() => void write.overwrite()}
        onCancel={write.dismissConflict}
      />
      <CodeModal
        target={codeTarget}
        readOnly={readOnly}
        commit={write.commit}
        openAsFile={openCompanionFile}
        onClose={() => setCodeTarget(null)}
      />
      <ActionPalette
        open={compose.palette !== null}
        title={compose.paletteTitle}
        subtitle={compose.paletteSubtitle}
        onClose={compose.closePalette}
        onPick={compose.handlePick}
      />
    </div>
  )
}
