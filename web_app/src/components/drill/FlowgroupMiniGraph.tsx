import { useCallback, useEffect, useMemo } from 'react'
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
import { useUIStore } from '../../store/uiStore'
import { useElkLayout } from '../graph/useElkLayout'
import { useGraphSearch } from '../graph/useGraphSearch'
import { GraphSearchInput } from '../graph/GraphSearchInput'
import { FlowgroupNode } from '../graph/nodes/FlowgroupNode'
import { ExternalNode } from '../graph/nodes/ExternalNode'
import { DependencyEdge, EdgeMarkerDefs } from '../graph/edges/DependencyEdge'
import { LoadingSpinner } from '../common/LoadingSpinner'
import { EmptyState } from '../common/EmptyState'
import { computeExternalConnections } from '../../utils/externalConnections'

const nodeTypes: NodeTypes = {
  flowgroup: FlowgroupNode,
  external: ExternalNode,
}

const edgeTypes: EdgeTypes = {
  dependency: DependencyEdge,
}

export function FlowgroupMiniGraph({ pipeline }: { pipeline: string }) {
  const { openFlowgroupModal } = useUIStore()
  // Filtered graph for layout (only this pipeline's flowgroups)
  const { data, isLoading, error } = useDependencyGraph('flowgroup', pipeline)
  // Full graph for computing cross-pipeline external connections
  const { data: fullData } = useDependencyGraph('flowgroup')
  const { fitView } = useReactFlow()

  const apiNodes = useMemo(() => data?.nodes ?? [], [data])
  const apiEdges = useMemo(() => data?.edges ?? [], [data])

  // Compute external connections from the full (unfiltered) graph
  // so we can show badges on flowgroups that connect to other pipelines
  const extConns = useMemo(() => {
    if (!fullData?.nodes || !fullData?.edges) return undefined
    return computeExternalConnections(fullData.nodes, fullData.edges)
  }, [fullData])

  const { nodes: layoutNodes, edges: layoutEdges, isLayouting } = useElkLayout(apiNodes, apiEdges, extConns)

  const search = useGraphSearch(layoutNodes, layoutEdges)

  useEffect(() => {
    if (layoutNodes.length > 0 && !isLayouting) {
      const timer = setTimeout(() => fitView({ padding: 0.15, duration: 300 }), 50)
      return () => clearTimeout(timer)
    }
  }, [layoutNodes, isLayouting, fitView])

  const onNodeClick = useCallback(
    (_: React.MouseEvent, node: { id: string; type?: string }) => {
      if (node.type === 'flowgroup') {
        openFlowgroupModal(node.id, pipeline)
      }
    },
    [openFlowgroupModal, pipeline],
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
    return <LoadingSpinner className="h-full" />
  }

  if (error) {
    const is404 = error instanceof ApiError && error.status === 404
    return (
      <div className="flex h-full items-center justify-center">
        <EmptyState
          title={is404 ? 'Drill view not available' : 'Failed to load flowgroups'}
          message={
            is404
              ? "This graph level isn't supported by the server yet."
              : error.message
          }
        />
      </div>
    )
  }

  if (layoutNodes.length === 0) {
    return <EmptyState title="No flowgroups" message="This pipeline has no flowgroups." />
  }

  return (
    <div className="flex h-full flex-col">
      <div className="border-b border-border bg-card px-3 py-1.5">
        <GraphSearchInput
          query={search.query}
          onQueryChange={search.setQuery}
          onClear={search.clear}
          matchCount={search.matchCount}
          totalCount={search.totalCount}
          isSearchActive={search.isSearchActive}
          onFitToMatches={fitToMatches}
          placeholder="Search flowgroups..."
        />
      </div>
      <div className="flex-1">
        <ReactFlow
          nodes={search.nodes}
          edges={search.edges}
          nodeTypes={nodeTypes}
          edgeTypes={edgeTypes}
          onNodeClick={onNodeClick}
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
