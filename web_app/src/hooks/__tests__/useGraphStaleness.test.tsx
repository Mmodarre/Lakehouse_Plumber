import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import type { ReactNode } from 'react'
import { act, renderHook, waitFor } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'

vi.mock('sonner', () => ({
  toast: { error: vi.fn(), success: vi.fn(), dismiss: vi.fn(), info: vi.fn() },
}))

import { toast } from 'sonner'
import { useGraphStaleness } from '../useGraphStaleness'
import { useGraphStalenessStore } from '../../store/graphStalenessStore'

const mockToastError = vi.mocked(toast.error)

// ── fetch-level mocking ──────────────────────────────────────
//
// Stubs global fetch below the real api/client + api/dependencies modules
// so the assertions cover the actual wire contract (POST /refresh, GET
// /staleness), not a mocked helper.

const fetchMock = vi.fn<(url: string, init?: RequestInit) => Promise<Response>>()

function jsonRes(body: unknown): Response {
  return new Response(JSON.stringify(body), { status: 200 })
}

function stalenessBody(stale: boolean) {
  return { stale, fingerprint: stale ? 'fp-old' : 'fp-new', built_at: null }
}

/** Route by method: GET → staleness seed, POST → refresh. */
function serve(seedStale: boolean, refreshStale = false) {
  fetchMock.mockImplementation((_url, init) => {
    const method = init?.method ?? 'GET'
    if (method === 'GET') return Promise.resolve(jsonRes(stalenessBody(seedStale)))
    if (method === 'POST') return Promise.resolve(jsonRes(stalenessBody(refreshStale)))
    return Promise.reject(new Error(`unexpected ${method}`))
  })
}

function postCalls() {
  return fetchMock.mock.calls.filter(([, init]) => init?.method === 'POST')
}

let queryClient: QueryClient

function wrapper({ children }: { children: ReactNode }) {
  return <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
}

beforeEach(() => {
  queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  })
  fetchMock.mockReset()
  vi.stubGlobal('fetch', fetchMock)
  useGraphStalenessStore.setState({ isStale: false })
})

afterEach(() => {
  vi.unstubAllGlobals()
  vi.restoreAllMocks()
})

describe('useGraphStaleness', () => {
  it('seeds the stale flag from GET /staleness when the server is stale', async () => {
    serve(true)
    const { result } = renderHook(() => useGraphStaleness(), { wrapper })

    await waitFor(() => expect(result.current.isStale).toBe(true))
    expect(useGraphStalenessStore.getState().isStale).toBe(true)
  })

  it('does not mark stale when the server reports fresh', async () => {
    serve(false)
    const { result } = renderHook(() => useGraphStaleness(), { wrapper })

    // Let the seed query resolve.
    await waitFor(() => expect(fetchMock).toHaveBeenCalled())
    expect(result.current.isStale).toBe(false)
    expect(useGraphStalenessStore.getState().isStale).toBe(false)
  })

  it('refresh POSTs to /dependencies/refresh, invalidates graph keys, and clears the flag', async () => {
    serve(false)
    // Start stale (e.g. from an earlier graph-stale event).
    useGraphStalenessStore.setState({ isStale: true })
    const invalidateSpy = vi.spyOn(queryClient, 'invalidateQueries')

    const { result } = renderHook(() => useGraphStaleness(), { wrapper })
    await waitFor(() => expect(result.current.isStale).toBe(true))

    act(() => {
      result.current.refresh()
    })

    await waitFor(() => expect(result.current.isStale).toBe(false))

    const post = postCalls()
    expect(post).toHaveLength(1)
    expect(String(post[0][0])).toBe('/api/dependencies/refresh')

    const keys = invalidateSpy.mock.calls.map(([arg]) => arg?.queryKey)
    expect(keys).toContainEqual(['dep-graph'])
    expect(keys).toContainEqual(['execution-order'])
    expect(keys).toContainEqual(['circular-deps'])
    expect(useGraphStalenessStore.getState().isStale).toBe(false)
  })

  it('a failed refresh surfaces the error, keeps the stale flag set, and re-enables the button', async () => {
    // GET seed reports fresh (no-op); the POST rebuild fails (e.g. LHP-VAL-*).
    fetchMock.mockImplementation((_url, init) => {
      const method = init?.method ?? 'GET'
      if (method === 'GET') return Promise.resolve(jsonRes(stalenessBody(false)))
      if (method === 'POST') {
        return Promise.resolve(
          new Response(
            JSON.stringify({
              error: {
                code: 'LHP-VAL-001',
                category: 'validation',
                message: 'rebuild failed',
                details: '',
                suggestions: [],
                context: {},
                http_status: 500,
              },
            }),
            { status: 500 },
          ),
        )
      }
      return Promise.reject(new Error(`unexpected ${method}`))
    })
    // Start stale (from an earlier graph-stale event).
    useGraphStalenessStore.setState({ isStale: true })

    const { result } = renderHook(() => useGraphStaleness(), { wrapper })
    await waitFor(() => expect(result.current.isStale).toBe(true))

    act(() => {
      result.current.refresh()
    })

    // The failure is surfaced via the project's toast helper.
    await waitFor(() => expect(mockToastError).toHaveBeenCalledWith('rebuild failed'))
    // Button re-enables (refresh no longer pending).
    await waitFor(() => expect(result.current.isRefreshing).toBe(false))
    // The stale flag stays SET — the graph is still stale.
    expect(result.current.isStale).toBe(true)
    expect(useGraphStalenessStore.getState().isStale).toBe(true)
  })
})
