import type { ReactNode } from 'react'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import { fireEvent, render, screen } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'

import { DependencyGraphWithControls } from '../DependencyGraph'
import { useWorkspaceStore } from '../../../store/workspaceStore'
import { useUIStore } from '../../../store/uiStore'

// Item 8: double-clicking a flowgroup node on the project map opens/focuses its
// entity tab. The heavy graph deps are stubbed; the REAL useElkLayout mapping +
// the double-click handler (flowgroup lookup → openEntityTab) run.

const rf = vi.hoisted(() => ({ fitView: vi.fn() }))

vi.mock('@xyflow/react', () => {
  type StubNode = { id: string; type?: string; data: Record<string, unknown> }
  return {
    ReactFlow: ({
      nodes,
      onNodeDoubleClick,
      children,
    }: {
      nodes: StubNode[]
      onNodeDoubleClick?: (e: unknown, node: StubNode) => void
      children?: ReactNode
    }) => (
      <div data-testid="react-flow">
        {nodes.map((n) => (
          <button
            key={n.id}
            data-testid={`rf-node-${n.id}`}
            onDoubleClick={(e) => onNodeDoubleClick?.(e, n)}
          >
            {String(n.data.label ?? n.id)}
          </button>
        ))}
        {children}
      </div>
    ),
    ReactFlowProvider: ({ children }: { children?: ReactNode }) => <>{children}</>,
    Panel: ({ children }: { children?: ReactNode }) => <div>{children}</div>,
    Background: () => null,
    BackgroundVariant: { Dots: 'dots' },
    Controls: () => null,
    MiniMap: () => null,
    Handle: () => null,
    EdgeLabelRenderer: ({ children }: { children?: ReactNode }) => <>{children}</>,
    BaseEdge: () => null,
    Position: { Left: 'left', Right: 'right', Top: 'top', Bottom: 'bottom' },
    MarkerType: { ArrowClosed: 'arrowclosed' },
    useReactFlow: () => ({ fitView: rf.fitView }),
    getBezierPath: () => ['', 0, 0, 0, 0] as const,
  }
})

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

vi.mock('../../../hooks/useDependencyGraph', () => ({
  useDependencyGraph: () => ({
    data: {
      nodes: [
        {
          id: 'bronze:orders',
          type: 'flowgroup',
          label: 'orders',
          pipeline: 'bronze',
          flowgroup: 'orders',
          stage: '',
          metadata: {},
        },
      ],
      edges: [],
    },
    isLoading: false,
    error: null,
  }),
  useCrossPipelineConnections: () => ({ data: undefined }),
  useCircularDeps: () => ({ data: undefined }),
}))

vi.mock('../../../hooks/useMapEnrichment', () => ({
  useMapEnrichment: () => ({ enrichNode: () => ({}), severityFor: () => undefined }),
}))

vi.mock('../../../hooks/useFlowgroups', () => ({
  useFlowgroups: () => ({
    data: {
      flowgroups: [
        {
          name: 'orders',
          pipeline: 'bronze',
          source_file: 'pipelines/bronze/orders.yaml',
          action_types: [],
        },
      ],
    },
  }),
}))

vi.mock('../../sandbox/useSandboxScope', () => ({ useSandboxScope: () => null }))

beforeEach(() => {
  vi.clearAllMocks()
  useUIStore.setState({ pipelineFilter: null, selectedEnv: 'dev' })
})

describe('DependencyGraphWithControls — flowgroup node activation (item 8)', () => {
  it('double-clicking a flowgroup node opens its entity tab', async () => {
    const openEntityTab = vi.fn()
    useWorkspaceStore.setState({ openEntityTab })

    const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } })
    const wrapper = ({ children }: { children: ReactNode }) => (
      <QueryClientProvider client={qc}>{children}</QueryClientProvider>
    )
    render(<DependencyGraphWithControls />, { wrapper })

    const node = await screen.findByTestId('rf-node-bronze:orders')
    fireEvent.doubleClick(node)

    // view is no longer passed — map node-open uses openEntityTab's new default.
    expect(openEntityTab).toHaveBeenCalledWith('bronze', 'orders', 'pipelines/bronze/orders.yaml')
  })
})
