import { describe, expect, it, vi, beforeEach } from 'vitest'
import { renderHook, waitFor } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import type { ReactNode } from 'react'
import { useLineage } from '../useLineage'
import { fetchLineage } from '../../api/lineage'
import type { DatasetLineageResponse } from '../../types/api'

vi.mock('../../api/lineage', () => ({ fetchLineage: vi.fn() }))
const fetchLineageMock = vi.mocked(fetchLineage)

function createWrapper() {
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } })
  return function Wrapper({ children }: { children: ReactNode }) {
    return <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
  }
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
    nodes: [],
    edges: [],
    consumers: [],
    warnings: [],
    stale: false,
    ...over,
  }
}

beforeEach(() => vi.clearAllMocks())

describe('useLineage', () => {
  it('fetches under the ["lineage", env, fqn] key with both params', async () => {
    fetchLineageMock.mockResolvedValue(response())
    const { result } = renderHook(() => useLineage('dev', 'main.bronze.customers'), {
      wrapper: createWrapper(),
    })
    await waitFor(() => expect(result.current.isSuccess).toBe(true))
    expect(fetchLineageMock).toHaveBeenCalledWith('dev', 'main.bronze.customers')
    expect(result.current.data?.fqn).toBe('main.bronze.customers')
  })

  it('is disabled until both env and fqn are present', () => {
    renderHook(() => useLineage('', 'main.x'), { wrapper: createWrapper() })
    renderHook(() => useLineage('dev', ''), { wrapper: createWrapper() })
    expect(fetchLineageMock).not.toHaveBeenCalled()
  })
})
