import { beforeEach, describe, expect, it, vi } from 'vitest'
import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { FlowgroupMiniGraph } from '../FlowgroupMiniGraph'
import { useUIStore } from '../../../store/uiStore'
import { useWorkspaceStore } from '../../../store/workspaceStore'
import { useDependencyGraph, useCrossPipelineConnections } from '../../../hooks/useDependencyGraph'
import { useFlowgroups } from '../../../hooks/useFlowgroups'
import { crossPipelineSummaryToMap } from '../../../utils/externalConnections'
import type { CrossPipelineSummary, GraphNode } from '../../../types/api'

// Captured across the module mocks below (vi.mock factories are hoisted, so
// shared state must come from vi.hoisted).
const rf = vi.hoisted(() => ({
  lastNodes: [] as Array<{ id: string; type?: string; data: Record<string, unknown> }>,
  fitView: vi.fn(),
}))

// React Flow itself is irrelevant to the wiring under test and heavy in jsdom.
// The stub renders one button per node and forwards the component's real
// onNodeClick / onNodeDoubleClick with the full node object — exactly what
// React Flow hands the handlers at runtime.
vi.mock('@xyflow/react', () => {
  type StubNode = { id: string; type?: string; data: Record<string, unknown> }
  return {
    ReactFlow: ({
      nodes,
      onNodeClick,
      onNodeDoubleClick,
    }: {
      nodes: StubNode[]
      onNodeClick?: (e: unknown, node: StubNode) => void
      onNodeDoubleClick?: (e: unknown, node: StubNode) => void
    }) => {
      rf.lastNodes = nodes
      return (
        <div data-testid="react-flow">
          {nodes.map((n) => (
            <button
              key={n.id}
              data-testid={`rf-node-${n.id}`}
              onClick={(e) => onNodeClick?.(e, n)}
              onDoubleClick={(e) => onNodeDoubleClick?.(e, n)}
            >
              {String(n.data.label ?? n.id)}
            </button>
          ))}
        </div>
      )
    },
    Background: () => null,
    BackgroundVariant: { Dots: 'dots' },
    Controls: () => null,
    Handle: () => null,
    Position: { Left: 'left', Right: 'right', Top: 'top', Bottom: 'bottom' },
    useReactFlow: () => ({ fitView: rf.fitView }),
    getBezierPath: () => ['', 0, 0, 0, 0] as const,
  }
})

// elk-api spawns a Web Worker at module load (unavailable in jsdom). Echo the
// graph back with positions so the REAL useElkLayout mapping — including the
// extConns lookup by node id — still runs.
vi.mock('elkjs/lib/elk-api.js', () => ({
  default: class {
    async layout(graph: { children?: Array<{ x?: number; y?: number }> }) {
      return {
        ...graph,
        children: (graph.children ?? []).map((c, i) => ({ ...c, x: i * 100, y: 0 })),
      }
    }
  },
}))

// GraphStaleBadge pulls react-query — irrelevant chrome here.
vi.mock('../../graph/GraphStaleBadge', () => ({ GraphStaleBadge: () => null }))

vi.mock('../../../hooks/useDependencyGraph', () => ({
  useDependencyGraph: vi.fn(),
  useCrossPipelineConnections: vi.fn(),
}))

vi.mock('../../../hooks/useFlowgroups', () => ({
  useFlowgroups: vi.fn(),
}))

const mockGraph = vi.mocked(useDependencyGraph)
const mockExtConns = vi.mocked(useCrossPipelineConnections)
const mockFlowgroups = vi.mocked(useFlowgroups)

// Backend node ids are pipeline-qualified; the bare flowgroup name rides on
// the node's `flowgroup` field (→ node.data.flowgroup after layout mapping).
const NODES: GraphNode[] = [
  { id: 'bronze.orders', type: 'flowgroup', label: 'orders', pipeline: 'bronze', flowgroup: 'orders', stage: 0 },
  { id: 'bronze.customers', type: 'flowgroup', label: 'customers', pipeline: 'bronze', flowgroup: 'customers', stage: 0 },
  { id: 'ext.raw_orders', type: 'external', label: 'raw_orders', pipeline: '', flowgroup: '', stage: 0 },
]

// The backend keys the cross-pipeline `connections` dict by the QUALIFIED id.
const EXT_SUMMARY: CrossPipelineSummary = {
  pipeline: 'bronze',
  connections: {
    'bronze.orders': [
      { direction: 'downstream', target: 'silver.orders_enriched', target_pipeline: 'silver' },
    ],
  },
}

/** Milliseconds the component waits for a possible second click (250ms) plus headroom. */
const AFTER_GRACE_MS = 350

function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

async function renderGraph() {
  render(<FlowgroupMiniGraph pipeline="bronze" />)
  // Layout resolves async — wait until the nodes materialise.
  return await screen.findByTestId('rf-node-bronze.orders')
}

beforeEach(() => {
  rf.lastNodes = []
  rf.fitView.mockClear()

  useWorkspaceStore.setState({ buffers: [], tabs: [], activePath: null })
  // The mini graph renders inside the pipeline drill modal.
  useUIStore.setState({ drillPipeline: 'bronze', drillFlowgroup: null })

  mockGraph.mockReturnValue({
    data: { nodes: NODES, edges: [] },
    isLoading: false,
    error: null,
  } as unknown as ReturnType<typeof useDependencyGraph>)
  mockExtConns.mockReturnValue({
    // Real mapping utility on a qualified-id-keyed payload — the exact shape
    // the badge layer receives from useCrossPipelineConnections' select.
    data: crossPipelineSummaryToMap(EXT_SUMMARY),
  } as unknown as ReturnType<typeof useCrossPipelineConnections>)
  mockFlowgroups.mockReturnValue({
    data: {
      flowgroups: [{ name: 'orders', source_file: 'pipelines/bronze/orders.yaml' }],
    },
  } as unknown as ReturnType<typeof useFlowgroups>)
})

describe('FlowgroupMiniGraph — qualified node ids', () => {
  it('single-click opens the drill modal with the BARE flowgroup name, not the qualified id', async () => {
    const user = userEvent.setup()
    const node = await renderGraph()

    await user.click(node)
    // Deferred one grace period so a double-click can cancel it.
    expect(useUIStore.getState().drillFlowgroup).toBeNull()

    await waitFor(() =>
      expect(useUIStore.getState().drillFlowgroup).toEqual({
        name: 'orders',
        pipeline: 'bronze',
      }),
    )
  })

  it('double-click opens the designer tab with the BARE flowgroup name and its source file', async () => {
    const user = userEvent.setup()
    const node = await renderGraph()

    await user.dblClick(node)

    const ws = useWorkspaceStore.getState()
    expect(ws.tabs).toEqual([
      {
        kind: 'designer',
        id: 'designer:bronze/orders',
        pipeline: 'bronze',
        flowgroup: 'orders',
        filePath: 'pipelines/bronze/orders.yaml',
      },
    ])
    expect(ws.activePath).toBe('designer:bronze/orders')

    // The drill stack closes so the canvas is visible, and the pending
    // single-click must have been cancelled — no modal opens after the grace.
    expect(useUIStore.getState().drillPipeline).toBeNull()
    await sleep(AFTER_GRACE_MS)
    expect(useUIStore.getState().drillFlowgroup).toBeNull()
  })

  it('attaches external-connection badges by the QUALIFIED node id', async () => {
    await renderGraph()

    // The backend connections dict key ('bronze.orders') must survive as the
    // Map key so useElkLayout's extConns.get(node.id) lookup hits.
    const orders = rf.lastNodes.find((n) => n.id === 'bronze.orders')
    expect(orders?.data.externalConnections).toEqual([
      { direction: 'downstream', targetNodeId: 'silver.orders_enriched', targetPipeline: 'silver' },
    ])

    const customers = rf.lastNodes.find((n) => n.id === 'bronze.customers')
    expect(customers?.data.externalConnections).toEqual([])
  })

  it('ignores clicks on non-flowgroup nodes', async () => {
    const user = userEvent.setup()
    await renderGraph()

    const external = screen.getByTestId('rf-node-ext.raw_orders')
    await user.click(external)
    await user.dblClick(external)
    await sleep(AFTER_GRACE_MS)

    expect(useUIStore.getState().drillFlowgroup).toBeNull()
    expect(useWorkspaceStore.getState().tabs).toEqual([])
  })
})

describe('crossPipelineSummaryToMap — qualified keys', () => {
  it('keys the map by the backend connections dict key (the qualified node id)', () => {
    const map = crossPipelineSummaryToMap(EXT_SUMMARY)
    expect([...map.keys()]).toEqual(['bronze.orders'])
    expect(map.get('bronze.orders')).toEqual([
      { direction: 'downstream', targetNodeId: 'silver.orders_enriched', targetPipeline: 'silver' },
    ])
    // The bare name is NOT a key — lookups must use node.id.
    expect(map.has('orders')).toBe(false)
  })
})
