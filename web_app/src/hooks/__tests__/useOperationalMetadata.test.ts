import { describe, expect, it, vi, beforeEach } from 'vitest'
import { renderHook, waitFor } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { createElement, type ReactNode } from 'react'
import { useOperationalMetadata } from '../useOperationalMetadata'
import { fetchOperationalMetadata } from '../../api/operationalMetadata'
import type { OperationalMetadataResponse } from '../../types/api'

vi.mock('../../api/operationalMetadata', () => ({
  fetchOperationalMetadata: vi.fn(),
}))
const fetchMock = vi.mocked(fetchOperationalMetadata)

function createWrapper() {
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } })
  return function Wrapper({ children }: { children: ReactNode }) {
    return createElement(QueryClientProvider, { client: queryClient }, children)
  }
}

function response(over: Partial<OperationalMetadataResponse> = {}): OperationalMetadataResponse {
  return {
    columns: [
      {
        name: '_ingested_at',
        expression: 'current_timestamp()',
        description: 'Ingestion timestamp',
        applies_to: ['streaming_table'],
        source: 'builtin',
      },
    ],
    presets: [{ name: 'audit', columns: ['_ingested_at'], description: null }],
    ...over,
  }
}

beforeEach(() => vi.clearAllMocks())

describe('useOperationalMetadata', () => {
  it('fetches under the ["operational-metadata"] key and returns parsed columns/presets', async () => {
    fetchMock.mockResolvedValue(response())
    const { result } = renderHook(() => useOperationalMetadata(), {
      wrapper: createWrapper(),
    })
    await waitFor(() => expect(result.current.isSuccess).toBe(true))
    expect(fetchMock).toHaveBeenCalledTimes(1)
    expect(result.current.data?.columns).toHaveLength(1)
    expect(result.current.data?.columns[0]?.name).toBe('_ingested_at')
    expect(result.current.data?.presets?.[0]?.name).toBe('audit')
  })

  it('exposes the standard { data, isLoading, error } query object', () => {
    fetchMock.mockResolvedValue(response())
    const { result } = renderHook(() => useOperationalMetadata(), {
      wrapper: createWrapper(),
    })
    expect(result.current).toHaveProperty('data')
    expect(result.current).toHaveProperty('isLoading')
    expect(result.current).toHaveProperty('error')
  })
})
