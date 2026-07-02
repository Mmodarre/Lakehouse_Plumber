import { useCallback, useEffect, useState } from 'react'
import ELK, { type ElkNode, type ElkExtendedEdge } from 'elkjs/lib/elk.bundled.js'
import type { Node, Edge } from '@xyflow/react'
import type { GraphNode, GraphEdge } from '../../types/api'
import type { ExternalConnection } from '../../types/graph'
import { computeExternalConnections } from '../../utils/externalConnections'

const elk = new ELK()

// Node dimensions — flowgroup names are longer, need wider cards
const ACTION_NODE_WIDTH = 240
const FLOWGROUP_NODE_WIDTH = 300

function getNodeWidth(apiNode: GraphNode): number {
  return apiNode.type === 'flowgroup' ? FLOWGROUP_NODE_WIDTH : ACTION_NODE_WIDTH
}

function getNodeHeight(apiNode: GraphNode): number {
  // Write actions with mode/scd_type get an extra info row (+28px)
  if (apiNode.type === 'write') {
    const hasInfo = apiNode.metadata?.write_mode || apiNode.metadata?.scd_type != null
    if (hasInfo) return 90
  }
  // label(18) + card(40) = ~58, with padding ~62
  return 62
}

function toElkGraph(
  nodes: GraphNode[],
  edges: GraphEdge[],
): ElkNode {
  const elkNodes: ElkNode[] = nodes.map((n) => ({
    id: n.id,
    width: getNodeWidth(n),
    height: getNodeHeight(n),
    layoutOptions: {
      'partitioning.partition': String(n.stage ?? 0),
    },
  }))

  const elkEdges: ElkExtendedEdge[] = edges.map((e, i) => ({
    id: `e-${i}`,
    sources: [e.source],
    targets: [e.target],
  }))

  return {
    id: 'root',
    children: elkNodes,
    edges: elkEdges,
    layoutOptions: {
      'elk.algorithm': 'layered',
      'elk.direction': 'RIGHT',
      'elk.spacing.nodeNode': '40',
      'elk.layered.spacing.nodeNodeBetweenLayers': '80',
      'elk.edgeRouting': 'SPLINES',
      'elk.partitioning.activate': 'true',
      'elk.layered.crossingMinimization.strategy': 'LAYER_SWEEP',
      'elk.layered.nodePlacement.strategy': 'NETWORK_SIMPLEX',
    },
  }
}

// Map API type to our registered React Flow node type
const KNOWN_NODE_TYPES = new Set(['pipeline', 'flowgroup', 'action', 'external', 'load', 'write', 'transform', 'test'])

function resolveNodeType(apiType: string): string {
  if (KNOWN_NODE_TYPES.has(apiType)) return apiType
  // "unknown" or unrecognized → use "flowgroup" as default (most common)
  return 'flowgroup'
}

function toReactFlowNodes(
  elkRoot: ElkNode,
  apiNodes: GraphNode[],
  extConns?: Map<string, ExternalConnection[]>,
): Node[] {
  const nodeMap = new Map(apiNodes.map((n) => [n.id, n]))

  return (elkRoot.children ?? []).map((elkNode) => {
    const apiNode = nodeMap.get(elkNode.id)!
    return {
      id: elkNode.id,
      type: resolveNodeType(apiNode.type),
      position: { x: elkNode.x ?? 0, y: elkNode.y ?? 0 },
      data: {
        label: apiNode.label,
        pipeline: apiNode.pipeline,
        flowgroup: apiNode.flowgroup,
        stage: apiNode.stage,
        nodeType: apiNode.type,
        metadata: apiNode.metadata,
        ...apiNode.metadata,
        externalConnections: extConns?.get(elkNode.id) ?? [],
      },
    }
  })
}

function toReactFlowEdges(apiEdges: GraphEdge[]): Edge[] {
  return apiEdges.map((e, i) => ({
    id: `e-${i}`,
    source: e.source,
    target: e.target,
    type: 'dependency',
    data: { edgeType: e.type },
  }))
}

export function useElkLayout(
  apiNodes: GraphNode[],
  apiEdges: GraphEdge[],
  externalConnectionsOverride?: Map<string, ExternalConnection[]>,
) {
  const [nodes, setNodes] = useState<Node[]>([])
  const [edges, setEdges] = useState<Edge[]>([])
  const [isLayouting, setIsLayouting] = useState(false)

  const runLayout = useCallback(async () => {
    if (apiNodes.length === 0) {
      setNodes([])
      setEdges([])
      return
    }

    setIsLayouting(true)
    try {
      const elkGraph = toElkGraph(apiNodes, apiEdges)
      const layouted = await elk.layout(elkGraph)
      const extConns = externalConnectionsOverride ?? computeExternalConnections(apiNodes, apiEdges)
      setNodes(toReactFlowNodes(layouted, apiNodes, extConns))
      setEdges(toReactFlowEdges(apiEdges))
    } catch (err) {
      console.error('ELK layout error:', err)
    } finally {
      setIsLayouting(false)
    }
  }, [apiNodes, apiEdges, externalConnectionsOverride])

  useEffect(() => {
    runLayout()
  }, [runLayout])

  return { nodes, edges, isLayouting }
}
