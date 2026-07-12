import { useCallback, useEffect, useMemo, useState } from 'react'
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
import '@xyflow/react/dist/style.css'
import { ChevronRight, Cloud } from 'lucide-react'

import { useDependencyGraph } from '../../hooks/useDependencyGraph'
import { useUIStore } from '../../store/uiStore'
import { useSandboxScope } from '../sandbox/useSandboxScope'
import { filterGraphForScope } from '../sandbox/scopeFilter'
import type { GraphNode } from '../../types/api'
import { cn } from '../../lib/utils'
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

// Disconnected external sources collapse into this band above the canvas
// instead of rendering as an orphan node grid inside it.
function ExternalSourcesBand({ nodes }: { nodes: GraphNode[] }) {
  const [expanded, setExpanded] = useState(false)

  if (nodes.length === 0) return null

  return (
    <div className="border-b border-border bg-card">
      <button
        type="button"
        onClick={() => setExpanded((v) => !v)}
        aria-expanded={expanded}
        className="flex w-full items-center gap-1.5 px-4 py-1.5 text-2xs font-medium text-muted-foreground transition-colors duration-150 hover:text-foreground"
      >
        <ChevronRight
          className={cn('size-3 transition-transform duration-150', expanded && 'rotate-90')}
          aria-hidden="true"
        />
        <Cloud className="size-3 text-node-external" aria-hidden="true" />
        <span>External sources ({nodes.length})</span>
      </button>
      {expanded && (
        <div className="flex flex-wrap gap-1.5 px-4 pb-2">
          {nodes.map((n) => (
            <span
              key={n.id}
              title={n.label}
              className="inline-flex max-w-75 items-center gap-1 rounded-sm border border-border bg-muted/50 px-1.5 py-0.5 font-mono text-2xs text-muted-foreground"
            >
              <Cloud className="size-3 shrink-0 text-node-external" aria-hidden="true" />
              <span className="truncate">{n.label}</span>
            </span>
          ))}
        </div>
      )}
    </div>
  )
}

interface GraphCanvasProps {
  nodes: Node[]
  edges: Edge[]
  onNodeClick: (event: React.MouseEvent, node: { id: string; type?: string }) => void
}

export function GraphCanvas({ nodes, edges, onNodeClick }: GraphCanvasProps) {
  return (
    <ReactFlow
      nodes={nodes}
      edges={edges}
      nodeTypes={nodeTypes}
      edgeTypes={edgeTypes}
      onNodeClick={onNodeClick}
      onlyRenderVisibleElements
      fitView
      minZoom={0.1}
      maxZoom={2}
      proOptions={{ hideAttribution: true }}
    >
      <EdgeMarkerDefs />
      {/* Dot grid + chrome colors come from the tokened .react-flow CSS block */}
      <Background variant={BackgroundVariant.Dots} gap={20} size={1.2} />
      <Controls showInteractive={false} position="top-right" />
      <MiniMap
        nodeStrokeWidth={1}
        nodeColor={minimapNodeColor}
        position="bottom-right"
      />
      <GraphLegend />
    </ReactFlow>
  )
}

export function DependencyGraphWithControls() {
  const { pipelineFilter, openPipelineModal } = useUIStore()
  const scope = useSandboxScope()
  // While sandbox mode narrows the scope, fetch the whole project and filter
  // client-side — the header pipeline filter is disabled in that mode.
  const { data, isLoading, error } = useDependencyGraph(
    'pipeline',
    scope ? undefined : pipelineFilter ?? undefined,
  )
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

  // Fit view after layout completes
  useEffect(() => {
    if (layoutNodes.length > 0 && !isLayouting) {
      const timer = setTimeout(() => fitView({ padding: 0.12, duration: 300 }), 50)
      return () => clearTimeout(timer)
    }
  }, [layoutNodes, isLayouting, fitView])

  const onNodeClick = useCallback(
    (_: React.MouseEvent, node: { id: string; type?: string }) => {
      if (node.type === 'pipeline') {
        openPipelineModal(node.id)
      }
    },
    [openPipelineModal],
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
      <ExternalSourcesBand nodes={bandedExternals} />
      <div className="flex-1">
        <GraphCanvas
          nodes={search.nodes}
          edges={search.edges}
          onNodeClick={onNodeClick}
        />
      </div>
    </div>
  )
}
