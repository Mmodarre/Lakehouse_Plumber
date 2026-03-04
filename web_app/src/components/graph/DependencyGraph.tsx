import { useCallback, useEffect, useMemo } from 'react'
import {
  ReactFlow,
  MiniMap,
  Controls,
  Background,
  BackgroundVariant,
  useReactFlow,
  type NodeTypes,
  type EdgeTypes,
  type Node,
  type Edge,
} from '@xyflow/react'
import '@xyflow/react/dist/style.css'

import { useDependencyGraph } from '../../hooks/useDependencyGraph'
import { useUIStore } from '../../store/uiStore'
import { useElkLayout } from './useElkLayout'
import { useGraphSearch } from './useGraphSearch'
import { GraphControls } from './GraphControls'
import { PipelineNode } from './nodes/PipelineNode'
import { ExternalNode } from './nodes/ExternalNode'
import { DependencyEdge } from './edges/DependencyEdge'
import { LoadingSpinner } from '../common/LoadingSpinner'
import { EmptyState } from '../common/EmptyState'

const nodeTypes: NodeTypes = {
  pipeline: PipelineNode,
  external: ExternalNode,
}

const edgeTypes: EdgeTypes = {
  dependency: DependencyEdge,
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
      fitView
      minZoom={0.1}
      maxZoom={2}
      proOptions={{ hideAttribution: true }}
      style={{ background: '#ffffff' }}
    >
      {/* Arrow markers for edge types */}
      <svg>
        <defs>
          <marker id="arrow" viewBox="0 0 10 10" refX="9" refY="5"
            markerWidth="7" markerHeight="7" orient="auto-start-reverse"
          >
            <path d="M 0 0 L 10 5 L 0 10 z" fill="#94a3b8" />
          </marker>
          <marker id="arrow-cross-pipeline" viewBox="0 0 10 10" refX="9" refY="5"
            markerWidth="7" markerHeight="7" orient="auto-start-reverse"
          >
            <path d="M 0 0 L 10 5 L 0 10 z" fill="#f59e0b" />
          </marker>
        </defs>
      </svg>
      {/* Subtle dot grid matching Databricks canvas */}
      <Background
        variant={BackgroundVariant.Dots}
        gap={22}
        size={1.2}
        color="#dcdcdc"
      />
      {/* Controls on the right side */}
      <Controls
        showInteractive={false}
        position="top-right"
        className="!shadow-sm !border-slate-200 !rounded-md"
      />
      <MiniMap
        nodeStrokeWidth={1}
        nodeColor="#e2e8f0"
        position="bottom-right"
        className="!border-slate-200 !shadow-sm !rounded-md"
        maskColor="rgba(255, 255, 255, 0.85)"
      />
    </ReactFlow>
  )
}

export function DependencyGraphWithControls() {
  const { pipelineFilter, openPipelineModal, openFlowgroupBuilder: openCreateFlowgroupDialog } = useUIStore()
  const { data, isLoading, error } = useDependencyGraph(
    'pipeline',
    pipelineFilter ?? undefined,
  )
  const { fitView } = useReactFlow()

  const apiNodes = useMemo(() => data?.nodes ?? [], [data])
  const apiEdges = useMemo(() => data?.edges ?? [], [data])
  const { nodes: layoutNodes, edges: layoutEdges, isLayouting } = useElkLayout(apiNodes, apiEdges)

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

  if (isLoading || isLayouting) {
    return (
      <div className="flex h-full flex-col">
        <GraphControls
          query={search.query}
          onQueryChange={search.setQuery}
          onClear={search.clear}
          matchCount={search.matchCount}
          totalCount={search.totalCount}
          isSearchActive={search.isSearchActive}
          onCreateFlowgroup={openCreateFlowgroupDialog}
          placeholder="Search pipelines..."
        />
        <LoadingSpinner className="flex-1" />
      </div>
    )
  }

  if (error) {
    return (
      <div className="flex h-full flex-col">
        <GraphControls
          query={search.query}
          onQueryChange={search.setQuery}
          onClear={search.clear}
          matchCount={search.matchCount}
          totalCount={search.totalCount}
          isSearchActive={search.isSearchActive}
          onCreateFlowgroup={openCreateFlowgroupDialog}
          placeholder="Search pipelines..."
        />
        <div className="flex flex-1 items-center justify-center text-sm text-red-500">
          Failed to load graph: {error.message}
        </div>
      </div>
    )
  }

  if (search.nodes.length === 0 && !search.isSearchActive) {
    return (
      <div className="flex h-full flex-col">
        <GraphControls
          query={search.query}
          onQueryChange={search.setQuery}
          onClear={search.clear}
          matchCount={search.matchCount}
          totalCount={search.totalCount}
          isSearchActive={search.isSearchActive}
          onCreateFlowgroup={openCreateFlowgroupDialog}
          placeholder="Search pipelines..."
        />
        <EmptyState title="No dependencies" message="Run 'lhp generate' to create pipelines first." />
      </div>
    )
  }

  return (
    <div className="flex h-full flex-col">
      <GraphControls
        query={search.query}
        onQueryChange={search.setQuery}
        onClear={search.clear}
        matchCount={search.matchCount}
        totalCount={search.totalCount}
        isSearchActive={search.isSearchActive}
        onFitToMatches={fitToMatches}
        onCreateFlowgroup={openCreateFlowgroupDialog}
        placeholder="Search pipelines..."
      />
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
