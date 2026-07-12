import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import type { ReactNode } from 'react'
import { renderHook, render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'

import { useCrossPipelineConnections } from '../useDependencyGraph'
import { crossPipelineSummaryToMap } from '../../utils/externalConnections'
import { ExternalBadge } from '../../components/graph/badges/ExternalBadge'
import type { CrossPipelineSummary } from '../../types/api'

const { openPipelineModal } = vi.hoisted(() => ({ openPipelineModal: vi.fn() }))
vi.mock('@/store/uiStore', () => ({ useUIStore: () => ({ openPipelineModal }) }))

// ── fetch-level mocking ──────────────────────────────────────
// Stub global fetch below the real api/client + api/dependencies modules so
// the assertions cover the actual wire contract (GET /dependencies/cross-
// pipeline?pipeline=X) and the hook's select transform end-to-end.

const fetchMock = vi.fn<(url: string, init?: RequestInit) => Promise<Response>>()

const SUMMARY: CrossPipelineSummary = {
  pipeline: 'bronze',
  connections: {
    orders_fg: [{ direction: 'downstream', target: 'orders_silver', target_pipeline: 'silver' }],
    events_fg: [{ direction: 'upstream', target: 'raw_events_source', target_pipeline: '' }],
  },
}

let queryClient: QueryClient

function wrapper({ children }: { children: ReactNode }) {
  return <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
}

beforeEach(() => {
  queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } })
  fetchMock.mockReset()
  fetchMock.mockResolvedValue(new Response(JSON.stringify(SUMMARY), { status: 200 }))
  vi.stubGlobal('fetch', fetchMock)
  openPipelineModal.mockClear()
})

afterEach(() => {
  vi.unstubAllGlobals()
  vi.restoreAllMocks()
})

describe('crossPipelineSummaryToMap', () => {
  it('maps the compact wire fields onto ExternalConnection (target→targetNodeId, target_pipeline→targetPipeline)', () => {
    const map = crossPipelineSummaryToMap(SUMMARY)
    expect(map.get('orders_fg')).toEqual([
      { direction: 'downstream', targetNodeId: 'orders_silver', targetPipeline: 'silver' },
    ])
    expect(map.get('events_fg')).toEqual([
      { direction: 'upstream', targetNodeId: 'raw_events_source', targetPipeline: '' },
    ])
  })

  it('returns an empty map when the summary carries no connections', () => {
    expect(crossPipelineSummaryToMap({ pipeline: 'bronze' }).size).toBe(0)
  })
})

describe('useCrossPipelineConnections', () => {
  it('fetches the compact cross-pipeline endpoint for the given pipeline and returns a mapped Map', async () => {
    const { result } = renderHook(() => useCrossPipelineConnections('bronze'), { wrapper })

    await waitFor(() => expect(result.current.data).toBeInstanceOf(Map))

    const url = String(fetchMock.mock.calls[0][0])
    expect(url).toContain('/dependencies/cross-pipeline?pipeline=bronze')

    expect(result.current.data?.get('orders_fg')).toEqual([
      { direction: 'downstream', targetNodeId: 'orders_silver', targetPipeline: 'silver' },
    ])
  })

  it('feeds the compact result into the badge layer — badge drills into the connected pipeline', async () => {
    const { result } = renderHook(() => useCrossPipelineConnections('bronze'), { wrapper })
    await waitFor(() => expect(result.current.data).toBeInstanceOf(Map))

    const conns = result.current.data!.get('orders_fg')!
    render(<ExternalBadge connections={conns} />)

    const user = userEvent.setup()
    await user.click(screen.getByRole('button', { name: /external connection/i }))
    expect(screen.getByRole('menuitem')).toHaveTextContent('silver')
    await user.click(screen.getByRole('menuitem'))
    expect(openPipelineModal).toHaveBeenCalledWith('silver')
  })
})
