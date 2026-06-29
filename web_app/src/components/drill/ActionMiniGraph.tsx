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
import { useElkLayout } from '../graph/useElkLayout'
import { ActionNode } from '../graph/nodes/ActionNode'
import { ExternalNode } from '../graph/nodes/ExternalNode'
import { DependencyEdge } from '../graph/edges/DependencyEdge'
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
    return (
      <div className="flex h-full items-center justify-center text-sm text-red-500">
        Failed to load actions: {error.message}
      </div>
    )
  }

  if (nodes.length === 0) {
    return <EmptyState title="No actions" message="This flowgroup has no actions." />
  }

  return (
    <ReactFlow
      nodes={nodes}
      edges={edges}
      nodeTypes={nodeTypes}
      edgeTypes={edgeTypes}
      fitView
      minZoom={0.1}
      maxZoom={2}
      proOptions={{ hideAttribution: true }}
      style={{ background: '#fafafa' }}
    >
      <svg>
        <defs>
          <marker id="arrow-action" viewBox="0 0 10 10" refX="9" refY="5"
            markerWidth="7" markerHeight="7" orient="auto-start-reverse"
          >
            <path d="M 0 0 L 10 5 L 0 10 z" fill="#94a3b8" />
          </marker>
        </defs>
      </svg>
      <Background variant={BackgroundVariant.Dots} gap={18} size={1} color="#e2e2e2" />
      <Controls showInteractive={false} position="top-right" className="!shadow-sm !border-slate-200 !rounded-md" />
    </ReactFlow>
  )
}
