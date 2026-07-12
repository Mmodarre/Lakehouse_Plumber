import { useEffect, useMemo } from 'react'
import {
  ReactFlow,
  Background,
  BackgroundVariant,
  Controls,
  useReactFlow,
  type NodeTypes,
  type EdgeTypes,
} from '@xyflow/react'

import { useDependencyGraph } from '../../hooks/useDependencyGraph'
import { ApiError } from '../../api/client'
import { useGraphStalenessStore } from '../../store/graphStalenessStore'
import { useElkLayout } from '../graph/useElkLayout'
import { GraphStaleBadge } from '../graph/GraphStaleBadge'
import { ActionNode } from '../graph/nodes/ActionNode'
import { ExternalNode } from '../graph/nodes/ExternalNode'
import { DependencyEdge, EdgeMarkerDefs } from '../graph/edges/DependencyEdge'
import { LoadingSpinner } from '../common/LoadingSpinner'
import { EmptyState } from '../common/EmptyState'

const nodeTypes: NodeTypes = {
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

export function ActionMiniGraph({ pipeline, flowgroup }: { pipeline: string; flowgroup: string }) {
  const { data, isLoading, error } = useDependencyGraph('action', pipeline)
  const { fitView } = useReactFlow()
  // No search header here — the stale badge gets a header bar only while stale.
  const isStale = useGraphStalenessStore((s) => s.isStale)

  // Filter nodes/edges to only those belonging to this flowgroup
  const filteredNodes = useMemo(() => {
    if (!data?.nodes) return []
    return data.nodes.filter((n) => n.flowgroup === flowgroup)
  }, [data, flowgroup])

  const filteredNodeIds = useMemo(
    () => new Set(filteredNodes.map((n) => n.id)),
    [filteredNodes],
  )

  const filteredEdges = useMemo(() => {
    if (!data?.edges) return []
    return data.edges.filter(
      (e) => filteredNodeIds.has(e.source) && filteredNodeIds.has(e.target),
    )
  }, [data, filteredNodeIds])

  const { nodes, edges, isLayouting } = useElkLayout(filteredNodes, filteredEdges)

  useEffect(() => {
    if (nodes.length > 0 && !isLayouting) {
      const timer = setTimeout(() => fitView({ padding: 0.15, duration: 300 }), 50)
      return () => clearTimeout(timer)
    }
  }, [nodes, isLayouting, fitView])

  if (isLoading || isLayouting) {
    return <LoadingSpinner className="h-full" />
  }

  if (error) {
    const is404 = error instanceof ApiError && error.status === 404
    return (
      <div className="flex h-full items-center justify-center">
        <EmptyState
          title={is404 ? 'Drill view not available' : 'Failed to load actions'}
          message={
            is404
              ? "This graph level isn't supported by the server yet."
              : error.message
          }
        />
      </div>
    )
  }

  if (nodes.length === 0) {
    return <EmptyState title="No actions" message="This flowgroup has no actions." />
  }

  return (
    <div className="flex h-full flex-col">
      {isStale && (
        <div className="flex items-center justify-end border-b border-border bg-card px-3 py-1.5">
          <GraphStaleBadge />
        </div>
      )}
      <div className="flex-1">
        <ReactFlow
          nodes={nodes}
          edges={edges}
          nodeTypes={nodeTypes}
          edgeTypes={edgeTypes}
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
        </ReactFlow>
      </div>
    </div>
  )
}
