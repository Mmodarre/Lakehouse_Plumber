import type { ReactNode } from 'react'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import { fireEvent, render, screen } from '@testing-library/react'

import { PipelineDagView } from '../PipelineDagView'
import { useWorkspaceStore } from '../../../../store/workspaceStore'
import { useUIStore } from '../../../../store/uiStore'

// PipelineDagView renders the SAME GraphCanvas machinery the project map uses,
// sourced from the per-pipeline flowgroup graph. The heavy graph deps are
// stubbed; the click handler (flowgroup lookup → openEntityTab) runs for real.

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
    useReactFlow: () => ({ fitView: vi.fn() }),
    getBezierPath: () => ['', 0, 0, 0, 0] as const,
  }
})

// Return the API nodes already mapped to React Flow nodes (skips the async ELK
// worker) so the click handler receives node.data.flowgroup deterministically.
vi.mock('../../../graph/useElkLayout', () => ({
  useElkLayout: (
    apiNodes: Array<{
      id: string
      type: string
      label: string
      pipeline: string
      flowgroup: string
    }>,
  ) => ({
    nodes: apiNodes.map((n) => ({
      id: n.id,
      type: n.type,
      position: { x: 0, y: 0 },
      data: { label: n.label, pipeline: n.pipeline, flowgroup: n.flowgroup, nodeType: n.type },
    })),
    edges: [],
    isLayouting: false,
  }),
}))

vi.mock('../../../../hooks/useDependencyGraph', () => ({
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
}))

vi.mock('../../../../hooks/useMapEnrichment', () => ({
  useMapEnrichment: () => ({
    enrichNode: () => ({}),
    severityFor: () => undefined,
    fqnHintFor: () => undefined,
  }),
}))

vi.mock('../../../../hooks/useFlowgroups', () => ({
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

beforeEach(() => {
  vi.clearAllMocks()
  useUIStore.setState({ selectedEnv: 'dev' })
})

describe('PipelineDagView', () => {
  it('clicking a flowgroup node opens its entity tab (source_file resolved)', () => {
    const openEntityTab = vi.fn()
    useWorkspaceStore.setState({ openEntityTab })

    render(<PipelineDagView pipeline="bronze" />)

    fireEvent.click(screen.getByTestId('rf-node-bronze:orders'))
    expect(openEntityTab).toHaveBeenCalledWith(
      'bronze',
      'orders',
      'pipelines/bronze/orders.yaml',
    )
  })
})
