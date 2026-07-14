import { useCallback, useEffect, useMemo, useState, type ReactNode } from 'react'
import {
  ReactFlow,
  MiniMap,
  Controls,
  Background,
  BackgroundVariant,
  Panel,
  useReactFlow,
  type NodeTypes,
  type EdgeTypes,
  type Node,
  type Edge,
} from '@xyflow/react'
// The React Flow base stylesheet is imported once at the app entry (main.tsx)
// so it is present regardless of which graph surface mounts first — do not
// re-import it here (see main.tsx).
import { Cloud } from 'lucide-react'

import { useDependencyGraph } from '../../hooks/useDependencyGraph'
import { useMapEnrichment } from '../../hooks/useMapEnrichment'
import { useFlowgroups } from '../../hooks/useFlowgroups'
import { useUIStore } from '../../store/uiStore'
import { useWorkspaceStore } from '../../store/workspaceStore'
import { useSandboxScope } from '../sandbox/useSandboxScope'
import { filterGraphForScope } from '../sandbox/scopeFilter'
import type { GraphNode } from '../../types/api'
import { Button } from '../ui/button'
import { Popover, PopoverContent, PopoverTrigger } from '../ui/popover'
import { useElkLayout } from './useElkLayout'
import { useGraphSearch } from './useGraphSearch'
import { GraphControls } from './GraphControls'
import { PipelineNode } from './nodes/PipelineNode'
import { FlowgroupNode } from './nodes/FlowgroupNode'
import { ActionNode } from './nodes/ActionNode'
import { ExternalNode } from './nodes/ExternalNode'
import { DependencyEdge, EdgeMarkerDefs } from './edges/DependencyEdge'
import { LoadingSpinner } from '../common/LoadingSpinner'
import { EmptyState } from '../common/EmptyState'

// Every level's node type is registered so flowgroup/action nodes render if
// the API returns them (in-canvas drill navigation itself is deferred).
const nodeTypes: NodeTypes = {
  pipeline: PipelineNode,
  flowgroup: FlowgroupNode,
  load: ActionNode,
  transform: ActionNode,
  write: ActionNode,
  test: ActionNode,
  action: ActionNode,
  external: ExternalNode,
}

const edgeTypes: EdgeTypes = {
  dependency: DependencyEdge,
}

// MiniMap fills keyed by the node identity/kind tokens so the overview stays
// legible in both themes.
const MINIMAP_NODE_COLORS: Record<string, string> = {
  pipeline: 'var(--node-pipeline)',
  flowgroup: 'var(--node-flowgroup)',
  external: 'var(--node-external)',
  load: 'var(--kind-load)',
  transform: 'var(--kind-transform)',
  write: 'var(--kind-write)',
  test: 'var(--kind-test)',
}

function minimapNodeColor(node: Node): string {
  return MINIMAP_NODE_COLORS[node.type ?? ''] ?? 'var(--node-external)'
}

function LegendSwatch({
  stroke,
  dash,
  opacity,
}: {
  stroke: string
  dash?: string
  opacity?: number
}) {
  return (
    <svg width="18" height="6" className="shrink-0" aria-hidden="true">
      <line
        x1="1"
        y1="3"
        x2="17"
        y2="3"
        strokeWidth="1.5"
        strokeDasharray={dash}
        style={{ stroke, strokeOpacity: opacity }}
      />
    </svg>
  )
}

function GraphLegend() {
  return (
    <Panel position="bottom-left">
      <div className="flex items-center gap-3 rounded-md border border-border bg-card/90 px-2.5 py-1.5 text-2xs text-muted-foreground backdrop-blur-sm">
        <span className="flex items-center gap-1.5">
          <LegendSwatch stroke="var(--edge)" />
          Internal
        </span>
        <span className="flex items-center gap-1.5">
          <LegendSwatch stroke="var(--edge-cross)" dash="4 3" />
          Cross-pipeline
        </span>
        <span className="flex items-center gap-1.5">
          <LegendSwatch stroke="var(--edge)" dash="6 3" opacity={0.6} />
          External
        </span>
      </div>
    </Panel>
  )
}

// Disconnected external sources fold into a single toolbar affordance (a
// popover) instead of a full-width band, keeping the map to one toolbar row.
function ExternalSourcesPopover({ nodes }: { nodes: GraphNode[] }) {
  const [open, setOpen] = useState(false)

  if (nodes.length === 0) return null

  return (
    <Popover open={open} onOpenChange={setOpen}>
      <PopoverTrigger asChild>
        <Button
          size="xs"
          variant="outline"
          aria-label={`External sources (${nodes.length})`}
          aria-expanded={open}
        >
          <Cloud className="text-node-external" aria-hidden="true" />
          External sources ({nodes.length})
        </Button>
      </PopoverTrigger>
      <PopoverContent align="start" className="max-h-80 w-80 overflow-auto p-2">
        <div className="flex flex-wrap gap-1.5">
          {nodes.map((n) => (
            <span
              key={n.id}
              title={n.label}
              className="inline-flex max-w-full items-center gap-1 rounded-sm border border-border bg-muted/50 px-1.5 py-0.5 font-mono text-2xs text-muted-foreground"
            >
              <Cloud className="size-3 shrink-0 text-node-external" aria-hidden="true" />
              <span className="truncate">{n.label}</span>
            </span>
          ))}
        </div>
      </PopoverContent>
    </Popover>
  )
}

interface GraphCanvasProps {
  nodes: Node[]
  edges: Edge[]
  onNodeClick: (event: React.MouseEvent, node: Node) => void
  onNodeDoubleClick?: (event: React.MouseEvent, node: Node) => void
}

export function GraphCanvas({ nodes, edges, onNodeClick, onNodeDoubleClick }: GraphCanvasProps) {
  return (
    <ReactFlow
      nodes={nodes}
      edges={edges}
      nodeTypes={nodeTypes}
      edgeTypes={edgeTypes}
      onNodeClick={onNodeClick}
      onNodeDoubleClick={onNodeDoubleClick}
      onlyRenderVisibleElements
      fitView
      minZoom={0.1}
      maxZoom={2}
      proOptions={{ hideAttribution: true }}
    >
      <EdgeMarkerDefs />
      {/* Dot grid + chrome colors come from the tokened .react-flow CSS block */}
      <Background variant={BackgroundVariant.Dots} gap={20} size={1.2} />
      <Controls showInteractive={false} position="bottom-right" />
      <MiniMap
        nodeStrokeWidth={1}
        nodeColor={minimapNodeColor}
        position="top-right"
      />
      <GraphLegend />
    </ReactFlow>
  )
}

export function DependencyGraphWithControls({ scopePicker }: { scopePicker?: ReactNode } = {}) {
  const { pipelineFilter, selectedEnv } = useUIStore()
  const openEntityTab = useWorkspaceStore((s) => s.openEntityTab)
  const openPipelineDag = useWorkspaceStore((s) => s.openPipelineDag)
  const { data: flowgroupData } = useFlowgroups()
  const scope = useSandboxScope()
  const [problemsOnly, setProblemsOnly] = useState(false)
  // The picker only offers in-scope pipelines, so honour its selection in both
  // modes; a stale out-of-scope filter (carried in when sandbox turned on)
  // clamps to "all in scope" rather than fetching a pipeline scope hides.
  const effectiveFilter =
    pipelineFilter && (!scope || scope.has(pipelineFilter)) ? pipelineFilter : undefined
  const { data, isLoading, error } = useDependencyGraph('pipeline', effectiveFilter)
  const { fitView } = useReactFlow()

  const scoped = useMemo(
    () => filterGraphForScope(data?.nodes ?? [], data?.edges ?? [], scope),
    [data, scope],
  )
  const apiNodes = scoped.nodes
  const apiEdges = scoped.edges

  const connectedIds = useMemo(() => {
    const ids = new Set<string>()
    for (const edge of apiEdges) {
      ids.add(edge.source)
      ids.add(edge.target)
    }
    return ids
  }, [apiEdges])

  const bandedExternals = useMemo(
    () => apiNodes.filter((n) => n.type === 'external' && !connectedIds.has(n.id)),
    [apiNodes, connectedIds],
  )

  const canvasApiNodes = useMemo(() => {
    if (bandedExternals.length === 0) return apiNodes
    const banded = new Set(bandedExternals.map((n) => n.id))
    return apiNodes.filter((n) => !banded.has(n.id))
  }, [apiNodes, bandedExternals])

  const { nodes: layoutNodes, edges: layoutEdges, isLayouting } = useElkLayout(canvasApiNodes, apiEdges)

  const search = useGraphSearch(layoutNodes, layoutEdges)

  // Client-side severity + produced-FQN join (§6.7 G2). Purely additive to node
  // `data`: a node with neither stays byte-identical, so nodes with no
  // enrichment render exactly as before.
  const enrichment = useMapEnrichment(selectedEnv)
  const enrichedNodes = useMemo(
    () =>
      search.nodes.map((n) => {
        const e = enrichment.enrichNode({
          nodeType: n.data.nodeType as string | undefined,
          pipeline: n.data.pipeline as string | undefined,
          flowgroup: n.data.flowgroup as string | undefined,
        })
        if (!e.severity && !e.fqn) return n
        return { ...n, data: { ...n.data, severity: e.severity, fqn: e.fqn } }
      }),
    [search.nodes, enrichment],
  )

  // Problems-only lens: dim (not remove) clean nodes/edges so the graph shape
  // is preserved while error/warning nodes stand out (mockup delta 11).
  const displayNodes = useMemo(() => {
    if (!problemsOnly) return enrichedNodes
    return enrichedNodes.map((n) =>
      n.data.severity ? n : { ...n, style: { ...(n.style ?? {}), opacity: 0.16 } },
    )
  }, [enrichedNodes, problemsOnly])
  const displayEdges = useMemo(() => {
    if (!problemsOnly) return search.edges
    return search.edges.map((e) => ({ ...e, style: { ...(e.style ?? {}), opacity: 0.35 } }))
  }, [search.edges, problemsOnly])

  // Fit view after layout completes
  useEffect(() => {
    if (layoutNodes.length > 0 && !isLayouting) {
      const timer = setTimeout(() => fitView({ padding: 0.12, duration: 300 }), 50)
      return () => clearTimeout(timer)
    }
  }, [layoutNodes, isLayouting, fitView])

  // Clicking a pipeline node drills into its flowgroup-level DAG in a new
  // center tab (replaces the retired drill modal).
  const onNodeClick = useCallback(
    (_: React.MouseEvent, node: Node) => {
      if (node.type === 'pipeline') {
        openPipelineDag(node.id)
      }
    },
    [openPipelineDag],
  )

  // Double-click a flowgroup node → open/focus its entity tab (matches
  // GraphView's node-activation pattern). The graph node carries pipeline +
  // flowgroup but not the file path, so resolve source_file from the flowgroups
  // list (the same source StructureLens opens entity tabs from).
  const onNodeDoubleClick = useCallback(
    (_: React.MouseEvent, node: Node) => {
      if (node.type !== 'flowgroup') return
      const pipeline = typeof node.data.pipeline === 'string' ? node.data.pipeline : ''
      const flowgroup = typeof node.data.flowgroup === 'string' ? node.data.flowgroup : ''
      if (flowgroup === '') return
      const summary = flowgroupData?.flowgroups.find(
        (f) => f.name === flowgroup && (pipeline === '' || f.pipeline === pipeline),
      )
      if (!summary?.source_file) return
      openEntityTab(summary.pipeline, summary.name, summary.source_file)
    },
    [openEntityTab, flowgroupData],
  )

  const fitToMatches = useCallback(() => {
    if (search.matchedNodeIds.length > 0) {
      fitView({
        nodes: search.matchedNodeIds.map((id) => ({ id })),
        padding: 0.3,
        duration: 400,
      })
    }
  }, [fitView, search.matchedNodeIds])

  const controls = (
    <GraphControls
      query={search.query}
      onQueryChange={search.setQuery}
      onClear={search.clear}
      matchCount={search.matchCount}
      totalCount={search.totalCount}
      isSearchActive={search.isSearchActive}
      onFitToMatches={fitToMatches}
      placeholder="Search pipelines..."
      scopePicker={scopePicker}
      externalSources={<ExternalSourcesPopover nodes={bandedExternals} />}
      problemsOnly={problemsOnly}
      onToggleProblemsOnly={() => setProblemsOnly((v) => !v)}
    />
  )

  if (isLoading || isLayouting) {
    return (
      <div className="flex h-full flex-col">
        {controls}
        <LoadingSpinner className="flex-1" />
      </div>
    )
  }

  if (error) {
    return (
      <div className="flex h-full flex-col">
        {controls}
        <div className="flex flex-1 items-center justify-center text-sm text-destructive">
          Failed to load graph: {error.message}
        </div>
      </div>
    )
  }

  if (search.nodes.length === 0 && bandedExternals.length === 0 && !search.isSearchActive) {
    return (
      <div className="flex h-full flex-col">
        {controls}
        <EmptyState title="No dependencies" message="Run 'lhp generate' to create pipelines first." />
      </div>
    )
  }

  return (
    <div className="flex h-full flex-col">
      {controls}
      <div className="flex-1">
        <GraphCanvas
          nodes={displayNodes}
          edges={displayEdges}
          onNodeClick={onNodeClick}
          onNodeDoubleClick={onNodeDoubleClick}
        />
      </div>
    </div>
  )
}
