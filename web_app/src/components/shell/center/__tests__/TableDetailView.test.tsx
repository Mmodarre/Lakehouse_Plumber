import type { ReactNode } from 'react'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { TableDetailView } from '../TableDetailView'
import { useUIStore } from '../../../../store/uiStore'
import { useWorkspaceStore } from '../../../../store/workspaceStore'
import { ApiError } from '../../../../api/client'
import { useLineage } from '../../../../hooks/useLineage'
import { useGraphStaleness } from '../../../../hooks/useGraphStaleness'
import type { DatasetLineageResponse } from '../../../../types/api'

vi.mock('../../../../hooks/useLineage', () => ({ useLineage: vi.fn() }))
const useLineageMock = vi.mocked(useLineage)

// useGraphStaleness is mocked so the 404 branch's stale/refresh affordance is
// deterministic without a live /api/dependencies/staleness fetch.
vi.mock('../../../../hooks/useGraphStaleness', () => ({ useGraphStaleness: vi.fn() }))
const useGraphStalenessMock = vi.mocked(useGraphStaleness)

const refreshMock = vi.fn()
function setStaleness(over: Partial<{ isStale: boolean; isRefreshing: boolean }> = {}) {
  useGraphStalenessMock.mockReturnValue({
    isStale: false,
    isRefreshing: false,
    refresh: refreshMock,
    ...over,
  })
}

type LineageResult = ReturnType<typeof useLineage>

function ok(data: DatasetLineageResponse): LineageResult {
  return { data, isLoading: false, error: null } as unknown as LineageResult
}
function loading(): LineageResult {
  return { data: undefined, isLoading: true, error: null } as unknown as LineageResult
}
function failed(error: unknown): LineageResult {
  return { data: undefined, isLoading: false, error } as unknown as LineageResult
}
function notFound(): LineageResult {
  return failed(
    new ApiError(404, {
      code: 'LHP-IO-000',
      category: 'io',
      message: 'not found',
      details: '',
      suggestions: [],
      context: {},
      http_status: 404,
    }),
  )
}

function response(over: Partial<DatasetLineageResponse> = {}): DatasetLineageResponse {
  return {
    fqn: 'main.bronze.customers',
    kind: 'table',
    pipeline: 'bronze',
    flowgroup: 'bronze_customers',
    action_name: 'write_customers',
    write_mode: 'streaming_table',
    scd_type: null,
    source_file: 'pipelines/bronze/customers.yaml',
    nodes: [
      { id: 'ext', kind: 'external', label: '/Volumes/main/raw/customers/', pipeline: '', flowgroup: '', dataset_fqn: '' },
      { id: 'load', kind: 'load', label: 'v_customers_raw', pipeline: 'bronze', flowgroup: 'bronze_customers', dataset_fqn: '' },
      { id: 'xf', kind: 'transform', label: 'v_customers_clean', pipeline: 'bronze', flowgroup: 'bronze_customers', dataset_fqn: '' },
      { id: 'w', kind: 'write', label: 'main.bronze.customers', pipeline: 'bronze', flowgroup: 'bronze_customers', dataset_fqn: 'main.bronze.customers' },
    ],
    edges: [
      { source: 'ext', target: 'load' },
      { source: 'load', target: 'xf' },
      { source: 'xf', target: 'w' },
    ],
    consumers: [
      { dataset_fqn: 'main.silver.customers', pipeline: 'silver', flowgroup: 'silver_customers', action_name: 'write_silver' },
    ],
    warnings: [],
    stale: false,
    ...over,
  }
}

function renderView(fqn: string) {
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false, staleTime: Infinity } },
  })
  const wrapper = ({ children }: { children: ReactNode }) => (
    <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
  )
  return render(<TableDetailView fqn={fqn} />, { wrapper })
}

beforeEach(() => {
  vi.clearAllMocks()
  setStaleness()
  useUIStore.setState({ selectedEnv: 'dev' })
  useWorkspaceStore.setState({ buffers: [], tabs: [], activePath: null, projectRoot: 'x', restoredDirtyCount: 0 })
})

describe('TableDetailView', () => {
  it('renders the header, lineage chain, producer and columns stub for a table', () => {
    useLineageMock.mockReturnValue(ok(response()))
    renderView('main.bronze.customers')

    // Header identity (fqn also appears as the focus rail card label).
    expect(screen.getByText('customers')).toBeInTheDocument()
    expect(screen.getAllByText('main.bronze.customers').length).toBeGreaterThan(0)
    // Lineage rail chain nodes (upstream views).
    expect(screen.getByText('v_customers_raw')).toBeInTheDocument()
    expect(screen.getByText('v_customers_clean')).toBeInTheDocument()
    expect(screen.getByText('/Volumes/main/raw/customers/')).toBeInTheDocument()
    // Produced-by action + jump-to-flowgroup.
    expect(screen.getByText('write_customers')).toBeInTheDocument()
    expect(screen.getByRole('button', { name: /Open flowgroup/ })).toBeInTheDocument()
    // Columns stub (exact §6.7 text).
    expect(
      screen.getByText(/Schema not available locally — LHP compiles pipelines/),
    ).toBeInTheDocument()
    // Consumer appears (rail + list).
    expect(screen.getAllByText('main.silver.customers').length).toBeGreaterThan(0)
  })

  it('opens the producing flowgroup as a YAML entity tab', async () => {
    useLineageMock.mockReturnValue(ok(response()))
    renderView('main.bronze.customers')

    await userEvent.click(screen.getByRole('button', { name: /Open flowgroup/ }))
    const tab = useWorkspaceStore.getState().tabs.find((t) => t.kind === 'entity')
    expect(tab).toMatchObject({
      kind: 'entity',
      pipeline: 'bronze',
      flowgroup: 'bronze_customers',
      filePath: 'pipelines/bronze/customers.yaml',
      // "Open flowgroup" jumps to the Code view (source YAML lives there now).
      view: 'code',
    })
  })

  it('renders a sink dataset with its sink id and Sink chip', () => {
    useLineageMock.mockReturnValue(
      ok(
        response({
          fqn: 'sink:kafka/out',
          kind: 'sink',
          nodes: [
            { id: 'w', kind: 'write', label: 'sink:kafka/out', pipeline: 'bronze', flowgroup: 'bronze_events', dataset_fqn: 'sink:kafka/out' },
          ],
          edges: [],
          consumers: [],
        }),
      ),
    )
    renderView('sink:kafka/out')

    expect(screen.getByText('kafka/out')).toBeInTheDocument()
    expect(screen.getAllByText('sink:kafka/out').length).toBeGreaterThan(0)
    expect(screen.getAllByText('Sink').length).toBeGreaterThan(0)
  })

  describe('404 not-found handling', () => {
    it('shows a neutral not-found (no delta-sink blame) for a plain table fqn when not stale', () => {
      setStaleness({ isStale: false })
      useLineageMock.mockReturnValue(notFound())
      renderView('main.bronze.missing')

      expect(screen.getByText('No lineage found for this dataset')).toBeInTheDocument()
      // A plain table fqn must NOT be blamed on delta-sink indexing.
      expect(screen.queryByText(/Delta sinks are indexed/i)).not.toBeInTheDocument()
      // No refresh affordance when the graph is not stale.
      expect(screen.queryByRole('button', { name: /Refresh/i })).not.toBeInTheDocument()
    })

    it('mentions the delta-sink limitation only for a sink: fqn when not stale', () => {
      setStaleness({ isStale: false })
      useLineageMock.mockReturnValue(notFound())
      renderView('sink:kafka/missing')

      expect(screen.getByText(/Delta sinks are indexed/i)).toBeInTheDocument()
    })

    it('offers a Refresh affordance (not delta-sink blame) on a 404 while the graph is stale', async () => {
      setStaleness({ isStale: true })
      useLineageMock.mockReturnValue(notFound())
      renderView('main.bronze.missing')

      // Stale-aware message: the graph may be behind, not "not produced".
      expect(screen.getByText(/dependency graph may be behind/i)).toBeInTheDocument()
      expect(screen.queryByText(/Delta sinks are indexed/i)).not.toBeInTheDocument()
      expect(screen.queryByText(/is not a produced dataset/i)).not.toBeInTheDocument()

      // The Refresh button drives useGraphStaleness().refresh().
      await userEvent.click(screen.getByRole('button', { name: /Refresh/i }))
      expect(refreshMock).toHaveBeenCalledTimes(1)
    })

    it('disables the Refresh button while a refresh is in flight', () => {
      setStaleness({ isStale: true, isRefreshing: true })
      useLineageMock.mockReturnValue(notFound())
      renderView('main.bronze.missing')

      expect(screen.getByRole('button', { name: /Refreshing/i })).toBeDisabled()
    })
  })

  it('renders index-health warnings (incl. multi-writer collision string) as a note', () => {
    useLineageMock.mockReturnValue(
      ok(
        response({
          warnings: ['main.bronze.customers is written by 2 actions across flowgroups'],
        }),
      ),
    )
    renderView('main.bronze.customers')
    expect(screen.getByText('Index health')).toBeInTheDocument()
    expect(screen.getByText(/written by 2 actions across flowgroups/)).toBeInTheDocument()
  })

  it('shows a loading spinner while the lineage query is in flight', () => {
    useLineageMock.mockReturnValue(loading())
    const { container } = renderView('main.bronze.customers')
    expect(container.querySelector('.animate-spin')).not.toBeNull()
  })
})
