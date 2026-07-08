import { describe, expect, it, vi, beforeEach } from 'vitest'
import { renderHook, waitFor } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import type { ReactNode } from 'react'
import { useRuns, useRun } from '../useRuns'
import { useBlueprints } from '../useBlueprints'
import { fetchRuns, fetchRun } from '../../api/runs'
import { fetchBlueprints } from '../../api/blueprints'

vi.mock('../../api/runs', () => ({
  fetchRuns: vi.fn(),
  fetchRun: vi.fn(),
}))
vi.mock('../../api/blueprints', () => ({
  fetchBlueprints: vi.fn(),
}))

const fetchRunsMock = vi.mocked(fetchRuns)
const fetchRunMock = vi.mocked(fetchRun)
const fetchBlueprintsMock = vi.mocked(fetchBlueprints)

function createWrapper() {
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  })
  return function Wrapper({ children }: { children: ReactNode }) {
    return <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
  }
}

beforeEach(() => {
  vi.clearAllMocks()
})

describe('useRuns', () => {
  it('fetches the run list under the push-invalidated run-history key', async () => {
    fetchRunsMock.mockResolvedValue({ runs: [], total: 0 })
    const { result } = renderHook(() => useRuns(25), { wrapper: createWrapper() })

    await waitFor(() => expect(result.current.isSuccess).toBe(true))
    expect(fetchRunsMock).toHaveBeenCalledWith(25)
    expect(result.current.data).toEqual({ runs: [], total: 0 })
  })
})

describe('useRun', () => {
  it('is disabled while runId is null', () => {
    renderHook(() => useRun(null), { wrapper: createWrapper() })
    expect(fetchRunMock).not.toHaveBeenCalled()
  })

  it('fetches the run detail with include_events', async () => {
    const detail = {
      run_id: 'r1',
      kind: 'validate',
      env: 'dev',
      pipeline: null,
      status: 'completed',
      started_at: '2026-07-07T00:00:00Z',
      finished_at: '2026-07-07T00:00:05Z',
      summary: null,
      issues: [],
      events: [],
    }
    fetchRunMock.mockResolvedValue(detail)
    const { result } = renderHook(() => useRun('r1', true), {
      wrapper: createWrapper(),
    })

    await waitFor(() => expect(result.current.isSuccess).toBe(true))
    expect(fetchRunMock).toHaveBeenCalledWith('r1', true)
    expect(result.current.data).toEqual(detail)
  })
})

describe('useBlueprints', () => {
  it('fetches blueprints with the include-instances flag', async () => {
    fetchBlueprintsMock.mockResolvedValue({ blueprints: [], total: 0 })
    const { result } = renderHook(() => useBlueprints(true), {
      wrapper: createWrapper(),
    })

    await waitFor(() => expect(result.current.isSuccess).toBe(true))
    expect(fetchBlueprintsMock).toHaveBeenCalledWith(true)
    expect(result.current.data).toEqual({ blueprints: [], total: 0 })
  })
})
