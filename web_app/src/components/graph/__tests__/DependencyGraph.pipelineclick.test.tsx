import type { ReactNode } from 'react'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import { fireEvent, render, screen } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'

import { DependencyGraphWithControls } from '../DependencyGraph'
import { useWorkspaceStore } from '../../../store/workspaceStore'
import { useUIStore } from '../../../store/uiStore'

// Fix #1: single-clicking a pipeline node on the project map opens its
// flowgroup-level DAG tab (openPipelineDag), replacing the retired drill modal.

const rf = vi.hoisted(() => ({ fitView: vi.fn() }))

vi.mock('@xyflow/react', () => {
  type StubNode = { id: string; type?: string; data: Record<string, unknown> }
  return {
    ReactFlow: ({
      nodes,
      onNodeClick,
      children,
    }: {
      nodes: StubNode[]
      onNodeClick?: (e: unknown, node: StubNode) => void
      children?: ReactNode
    }) => (
      <div data-testid="react-flow">
        {nodes.map((n) => (
          <button
            key={n.id}
            data-testid={`rf-node-${n.id}`}
            onClick={(e) => onNodeClick?.(e, n)}
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
          id: 'bronze',
          type: 'pipeline',
          label: 'bronze',
          pipeline: 'bronze',
          flowgroup: '',
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
  useFlowgroups: () => ({ data: { flowgroups: [] } }),
}))

vi.mock('../../sandbox/useSandboxScope', () => ({ useSandboxScope: () => null }))

beforeEach(() => {
  vi.clearAllMocks()
  useUIStore.setState({ pipelineFilter: null, selectedEnv: 'dev' })
})

describe('DependencyGraphWithControls — pipeline node activation (Fix #1)', () => {
  it('clicking a pipeline node opens its flowgroup-DAG tab', async () => {
    const openPipelineDag = vi.fn()
    useWorkspaceStore.setState({ openPipelineDag })

    const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } })
    const wrapper = ({ children }: { children: ReactNode }) => (
      <QueryClientProvider client={qc}>{children}</QueryClientProvider>
    )
    render(<DependencyGraphWithControls />, { wrapper })

    const node = await screen.findByTestId('rf-node-bronze')
    fireEvent.click(node)

    expect(openPipelineDag).toHaveBeenCalledWith('bronze')
  })
})
