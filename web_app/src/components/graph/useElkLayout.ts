import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import ELK, { type ElkNode, type ElkExtendedEdge } from 'elkjs/lib/elk-api.js'
import type { Node, Edge } from '@xyflow/react'
import type { GraphNode, GraphEdge } from '../../types/api'
import type { ExternalConnection } from '../../types/graph'
import { computeExternalConnections } from '../../utils/externalConnections'

// Run ELK layout in a Web Worker so large-graph layout never blocks the main
// thread. elk-worker.min.js is a classic (non-ESM) script, so the worker is
// pinned to { type: 'classic' } here rather than via a global worker.format:'es'
// in vite.config.ts — a global 'es' would break the Monaco ?worker chunks. The
// factory ignores elk-api's (undefined) workerUrl arg and lets Vite bundle the
// worker via the new URL(..., import.meta.url) pattern.
const elk = new ELK({
  workerFactory: () =>
    new Worker(new URL('elkjs/lib/elk-worker.min.js', import.meta.url), {
      type: 'classic',
    }),
})

// Node dimensions — must stay in sync with the NodeCard width classes
// (w-60 actions, w-75 flowgroups). Flowgroup names are longer, need wider cards.
const ACTION_NODE_WIDTH = 240
const FLOWGROUP_NODE_WIDTH = 300
// Icon chip + label + in-card sublabel (~52px card, no floating caption)
const NODE_HEIGHT = 56

function getNodeWidth(apiNode: GraphNode): number {
  return apiNode.type === 'flowgroup' ? FLOWGROUP_NODE_WIDTH : ACTION_NODE_WIDTH
}

function toElkGraph(
  nodes: GraphNode[],
  edges: GraphEdge[],
): ElkNode {
  const elkNodes: ElkNode[] = nodes.map((n) => ({
    id: n.id,
    width: getNodeWidth(n),
    height: NODE_HEIGHT,
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
      // Dimension hints for onlyRenderVisibleElements culling. ELK echoes back
      // the sizes it laid out with; fall back to the same constants so the cull
      // box matches the node box. initialWidth/initialHeight (not width/height)
      // so the wrapper releases to the content-sized card after measurement,
      // keeping <Handle> anchors flush on the variable-width max-w-60 nodes.
      initialWidth: elkNode.width ?? getNodeWidth(apiNode),
      initialHeight: elkNode.height ?? NODE_HEIGHT,
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

// Structural fingerprint of exactly the inputs that feed ELK geometry:
// node ids + widths (type-derived) + stage partitions, and edge endpoints.
// Labels, metadata, edge kinds, and externalConnections are deliberately
// excluded — they change node/edge *data*, never positions. Entries are
// sorted so an order-only reshuffle of the same graph reuses the cached
// layout instead of re-running ELK's order-sensitive layer sweep.
function layoutSignature(nodes: GraphNode[], edges: GraphEdge[]): string {
  const nodeSig = nodes
    .map((n) => `${n.id}\u001f${getNodeWidth(n)}\u001f${n.stage ?? 0}`)
    .sort()
  const edgeSig = edges.map((e) => `${e.source}\u001f${e.target}`).sort()
  return `${nodeSig.join('\u001e')}\u001d${edgeSig.join('\u001e')}`
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

  // Resolved once per input change rather than inside every layout run, so a
  // data-only refresh never re-derives the Map from scratch.
  const extConns = useMemo(
    () => externalConnectionsOverride ?? computeExternalConnections(apiNodes, apiEdges),
    [externalConnectionsOverride, apiNodes, apiEdges],
  )

  // Last computed geometry, keyed by layoutSignature. A re-run with an
  // unchanged structure (refetch identity churn, label/badge-only updates,
  // a new externalConnectionsOverride Map) skips elk.layout() and re-maps
  // the fresh data onto the cached positions.
  const layoutCacheRef = useRef<{ key: string; root: ElkNode } | null>(null)
  // Monotonic run id: the skip path resolves synchronously, so an older
  // still-in-flight elk.layout() must not clobber a newer run's result.
  const runSeqRef = useRef(0)

  const runLayout = useCallback(async () => {
    const seq = ++runSeqRef.current

    if (apiNodes.length === 0) {
      setNodes([])
      setEdges([])
      setIsLayouting(false)
      return
    }

    const key = layoutSignature(apiNodes, apiEdges)
    const cached = layoutCacheRef.current
    if (cached && cached.key === key) {
      setNodes(toReactFlowNodes(cached.root, apiNodes, extConns))
      setEdges(toReactFlowEdges(apiEdges))
      setIsLayouting(false)
      return
    }

    setIsLayouting(true)
    try {
      const elkGraph = toElkGraph(apiNodes, apiEdges)
      const layouted = await elk.layout(elkGraph)
      if (seq !== runSeqRef.current) return // superseded by a newer run
      layoutCacheRef.current = { key, root: layouted }
      setNodes(toReactFlowNodes(layouted, apiNodes, extConns))
      setEdges(toReactFlowEdges(apiEdges))
    } catch (err) {
      console.error('ELK layout error:', err)
    } finally {
      if (seq === runSeqRef.current) setIsLayouting(false)
    }
  }, [apiNodes, apiEdges, extConns])

  useEffect(() => {
    runLayout()
  }, [runLayout])

  return { nodes, edges, isLayouting }
}
