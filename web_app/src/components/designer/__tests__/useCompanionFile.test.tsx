import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import type { ReactNode } from 'react'
import { act, renderHook, waitFor } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'

vi.mock('sonner', () => ({ toast: { error: vi.fn(), success: vi.fn() } }))

import { companionCheckablePath, useCompanionFile } from '../useCompanionFile'

const fetchMock = vi.fn<(url: string, init?: RequestInit) => Promise<Response>>()

function notFound(): Response {
  return new Response(JSON.stringify({ detail: 'not found' }), { status: 404 })
}
function found(): Response {
  return new Response('SELECT 1', { status: 200, headers: { ETag: '"e1"' } })
}
function putOk(): Response {
  return new Response(JSON.stringify({ written: true, path: 'x', yaml_error: null, etag: 'e2' }), {
    status: 200,
  })
}

let queryClient: QueryClient
function wrapper({ children }: { children: ReactNode }) {
  return <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
}

beforeEach(() => {
  vi.clearAllMocks()
  vi.stubGlobal('fetch', fetchMock)
  queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } })
})
afterEach(() => vi.unstubAllGlobals())

describe('companionCheckablePath', () => {
  it('accepts a project-relative path', () => {
    expect(companionCheckablePath('sql/x.sql')).toBe('sql/x.sql')
  })
  it('rejects empty, absolute, and tokenized values', () => {
    expect(companionCheckablePath('')).toBeNull()
    expect(companionCheckablePath('   ')).toBeNull()
    expect(companionCheckablePath('/abs/x.sql')).toBeNull()
    expect(companionCheckablePath('C:\\x.sql')).toBeNull()
    expect(companionCheckablePath('${dir}/x.sql')).toBeNull()
    expect(companionCheckablePath(42)).toBeNull()
  })
})

describe('useCompanionFile', () => {
  it('reports missing on a 404 GET', async () => {
    fetchMock.mockResolvedValue(notFound())
    const { result } = renderHook(() => useCompanionFile('sql/missing.sql'), { wrapper })
    await waitFor(() => expect(result.current.status).toBe('missing'))
  })

  it('reports exists on a 200 GET', async () => {
    fetchMock.mockResolvedValue(found())
    const { result } = renderHook(() => useCompanionFile('sql/there.sql'), { wrapper })
    await waitFor(() => expect(result.current.status).toBe('exists'))
  })

  it('is unavailable (and makes no request) when there is no checkable path', () => {
    const { result } = renderHook(() => useCompanionFile(null), { wrapper })
    expect(result.current.status).toBe('unavailable')
    expect(fetchMock).not.toHaveBeenCalled()
  })

  it('create() PUTs the stub with the create-only If-Match, path, and body', async () => {
    fetchMock.mockImplementation((_url, init) =>
      Promise.resolve((init?.method ?? 'GET') === 'PUT' ? putOk() : notFound()),
    )
    const { result } = renderHook(() => useCompanionFile('sql/new.sql'), { wrapper })
    await waitFor(() => expect(result.current.status).toBe('missing'))

    let created: boolean | undefined
    await act(async () => {
      created = await result.current.create('-- SQL\n')
    })
    expect(created).toBe(true)

    const put = fetchMock.mock.calls.find(([, init]) => init?.method === 'PUT')!
    expect(put[0]).toBe('/api/files/sql/new.sql')
    expect((put[1]!.headers as Record<string, string>)['If-Match']).toBe('"create-only"')
    expect(JSON.parse(put[1]!.body as string)).toEqual({ content: '-- SQL\n' })
  })

  it('create(stub, atPath) PUTs to the proposed path even when the field is still empty', async () => {
    fetchMock.mockImplementation((_url, init) =>
      Promise.resolve((init?.method ?? 'GET') === 'PUT' ? putOk() : notFound()),
    )
    // A null hook path models an empty field; the caller proposes atPath.
    const { result } = renderHook(() => useCompanionFile(null), { wrapper })
    expect(result.current.status).toBe('unavailable')

    let created: boolean | undefined
    await act(async () => {
      created = await result.current.create('table: orders\n', 'schemas/orders.yaml')
    })
    expect(created).toBe(true)

    const put = fetchMock.mock.calls.find(([, init]) => init?.method === 'PUT')!
    expect(put[0]).toBe('/api/files/schemas/orders.yaml')
    expect((put[1]!.headers as Record<string, string>)['If-Match']).toBe('"create-only"')
    expect(JSON.parse(put[1]!.body as string)).toEqual({ content: 'table: orders\n' })
  })
})
