import type { GraphNode, GraphEdge } from '../types/api'
import type { ExternalConnection } from '../types/graph'

/**
 * Scans edges for external/cross-pipeline connections and builds
 * a map of nodeId → ExternalConnection[].
 */
export function computeExternalConnections(
  nodes: GraphNode[],
  edges: GraphEdge[],
): Map<string, ExternalConnection[]> {
  const nodeMap = new Map(nodes.map((n) => [n.id, n]))
  const result = new Map<string, ExternalConnection[]>()

  for (const edge of edges) {
    if (edge.type !== 'external' && edge.type !== 'cross_pipeline') continue

    const sourceNode = nodeMap.get(edge.source)
    const targetNode = nodeMap.get(edge.target)

    // Add downstream connection to the source node
    if (sourceNode && targetNode) {
      const sourceConns = result.get(edge.source) ?? []
      sourceConns.push({
        direction: 'downstream',
        targetNodeId: edge.target,
        targetPipeline: targetNode.pipeline,
      })
      result.set(edge.source, sourceConns)
    }

    // Add upstream connection to the target node
    if (targetNode && sourceNode) {
      const targetConns = result.get(edge.target) ?? []
      targetConns.push({
        direction: 'upstream',
        targetNodeId: edge.source,
        targetPipeline: sourceNode.pipeline,
      })
      result.set(edge.target, targetConns)
    }
  }

  return result
}
