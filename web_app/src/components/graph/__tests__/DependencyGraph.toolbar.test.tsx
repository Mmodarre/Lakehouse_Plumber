import type { ReactNode } from 'react'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { fireEvent, render, screen, within } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'

import { DependencyGraphWithControls } from '../DependencyGraph'
import { useUIStore } from '../../../store/uiStore'

// Item 11: the project map collapses to ONE decluttered toolbar row (search ·
// scope picker · problems-only · external-sources chip · new-flowgroup). No
// separate "Project map" title bar; no full-width "External sources" band.

const rf = vi.hoisted(() => ({ fitView: vi.fn() }))

vi.mock('@xyflow/react', () => {
  type StubNode = { id: string; type?: string; data: Record<string, unknown> }
  return {
    ReactFlow: ({ nodes, children }: { nodes: StubNode[]; children?: ReactNode }) => (
      <div data-testid="react-flow">
        {nodes.map((n) => (
          <div key={n.id} data-testid={`rf-node-${n.id}`} />
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

// A STABLE object identity across renders — a fresh literal each call would
// churn the scoped/layout memos and re-run the elk layout on every re-render
// (e.g. the problems-only toggle), never settling.
const graphResult = vi.hoisted(() => ({
  data: {
    nodes: [
      { id: 'bronze:orders', type: 'flowgroup', label: 'orders', pipeline: 'bronze', flowgroup: 'orders', stage: '', metadata: {} },
      { id: 'ext:s3', type: 'external', label: 's3://bucket/data', pipeline: '', flowgroup: '', stage: '', metadata: {} },
    ],
    edges: [],
  },
  isLoading: false,
  error: null,
}))

vi.mock('../../../hooks/useDependencyGraph', () => ({
  useDependencyGraph: () => graphResult,
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

function renderMap() {
  const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } })
  return render(<DependencyGraphWithControls scopePicker={<span>PICK-SCOPE</span>} />, {
    wrapper: ({ children }) => <QueryClientProvider client={qc}>{children}</QueryClientProvider>,
  })
}

beforeEach(() => {
  vi.clearAllMocks()
  useUIStore.setState({ pipelineFilter: null, selectedEnv: 'dev' })
  // Radix Popover needs pointer-capture APIs jsdom lacks.
  Element.prototype.scrollIntoView = vi.fn()
  Element.prototype.hasPointerCapture = vi.fn(() => false) as never
  Element.prototype.setPointerCapture = vi.fn()
  Element.prototype.releasePointerCapture = vi.fn()
  vi.stubGlobal(
    'ResizeObserver',
    class {
      observe() {}
      unobserve() {}
      disconnect() {}
    },
  )
})

afterEach(() => {
  vi.unstubAllGlobals()
})

describe('DependencyGraphWithControls — single decluttered toolbar (item 11)', () => {
  // One render, all structural assertions — mounting the real graph is heavy, so
  // the whole item-11 contract is checked against a single mounted toolbar.
  it('consolidates the map into ONE toolbar row: search · scope · problems-only · external-sources chip · new-flowgroup; no title bar or standalone external band', async () => {
    renderMap()

    const newFg = await screen.findByRole('button', { name: 'New flowgroup' })
    const toolbar = newFg.closest('div') as HTMLElement

    // All controls live in the SAME single toolbar row.
    expect(within(toolbar).getByPlaceholderText('Search pipelines...')).toBeInTheDocument()
    expect(within(toolbar).getByText('PICK-SCOPE')).toBeInTheDocument()
    const toggle = within(toolbar).getByRole('button', { name: 'Problems only' })
    const chip = within(toolbar).getByRole('button', { name: /External sources \(1\)/ })

    // The removed "Project map" title bar is gone.
    expect(screen.queryByText('Project map')).not.toBeInTheDocument()

    // External sources fold into a COLLAPSED toolbar chip, not an always-visible
    // full-width band: the trigger is closed and the source list is not rendered.
    expect(chip).toHaveAttribute('aria-expanded', 'false')
    expect(screen.queryByText('s3://bucket/data')).not.toBeInTheDocument()

    // Problems-only lens toggles.
    expect(toggle).toHaveAttribute('aria-pressed', 'false')
    fireEvent.click(toggle)
    expect(toggle).toHaveAttribute('aria-pressed', 'true')
  })
})
