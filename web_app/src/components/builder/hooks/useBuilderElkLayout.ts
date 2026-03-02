import ELK, { type ElkNode, type ElkExtendedEdge } from 'elkjs/lib/elk.bundled.js'
import type { ActionNodeConfig, BuilderEdge } from '../types/builder'

const elk = new ELK()

// Dimensions matching BuilderActionNode
const NODE_WIDTH = 180
const NODE_HEIGHT = 72

/**
 * Compute positions for deserialized builder nodes using ELK layered layout.
 * Returns a map from node ID to { x, y } position.
 */
export async function computeBuilderLayout(
  actionConfigs: ActionNodeConfig[],
  edges: BuilderEdge[],
): Promise<Map<string, { x: number; y: number }>> {
  if (actionConfigs.length === 0) {
    return new Map()
  }

  const elkNodes: ElkNode[] = actionConfigs.map((config) => ({
    id: config.id,
    width: NODE_WIDTH,
    height: NODE_HEIGHT,
  }))

  const elkEdges: ElkExtendedEdge[] = edges.map((e, i) => ({
    id: `elk-${i}`,
    sources: [e.source],
    targets: [e.target],
  }))

  const elkGraph: ElkNode = {
    id: 'root',
    children: elkNodes,
    edges: elkEdges,
    layoutOptions: {
      'elk.algorithm': 'layered',
      'elk.direction': 'RIGHT',
      'elk.spacing.nodeNode': '40',
      'elk.layered.spacing.nodeNodeBetweenLayers': '80',
      'elk.edgeRouting': 'SPLINES',
      'elk.layered.crossingMinimization.strategy': 'LAYER_SWEEP',
      'elk.layered.nodePlacement.strategy': 'NETWORK_SIMPLEX',
    },
  }

  try {
    const layouted = await elk.layout(elkGraph)
    const positions = new Map<string, { x: number; y: number }>()
    for (const child of layouted.children ?? []) {
      positions.set(child.id, { x: child.x ?? 0, y: child.y ?? 0 })
    }
    return positions
  } catch (err) {
    console.error('Builder ELK layout error:', err)
    // Fallback: stack nodes vertically
    const positions = new Map<string, { x: number; y: number }>()
    actionConfigs.forEach((config, i) => {
      positions.set(config.id, { x: 50, y: i * (NODE_HEIGHT + 40) })
    })
    return positions
  }
}
