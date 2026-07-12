import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import type { ReactNode } from 'react'
import { act, renderHook, waitFor } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'

vi.mock('sonner', () => ({
  toast: { error: vi.fn(), success: vi.fn(), dismiss: vi.fn() },
}))

import { deleteActionField, setActionField } from '@/lib/flowgroup-doc'
import { useDesignerWrite } from '../useDesignerWrite'

// ── fetch-level mocking (below api/client + api/files) ───────
// Assertions cover the real wire contract: the If-Match header and the
// serialized YAML body the PUT carries.

const PATH = 'pipelines/x.yaml'
const FLOWGROUP = 'fg'
const BASE = `pipeline: p
flowgroup: fg
actions:
  - name: a1
    type: transform
    transform_type: sql
    source: v_raw
    sql: SELECT 1
    target: v_out
`

const fetchMock = vi.fn<(url: string, init?: RequestInit) => Promise<Response>>()

function textRes(content: string, etag: string): Response {
  return new Response(content, { status: 200, headers: { ETag: `"${etag}"` } })
}
function putRes(etag = 'e2', yaml_error: unknown = null): Response {
  return new Response(JSON.stringify({ written: true, path: PATH, yaml_error, etag }), {
    status: 200,
  })
}
function staleRes(): Response {
  return new Response(JSON.stringify({ detail: 'File changed since read.' }), { status: 412 })
}

/** GET serves `diskContent`/`diskEtag`; PUTs drain a queue (default success). */
function serve(state: { diskContent?: string; diskEtag?: string; puts?: Response[] }) {
  fetchMock.mockImplementation((_url, init) => {
    const method = init?.method ?? 'GET'
    if (method === 'GET') {
      return Promise.resolve(textRes(state.diskContent ?? BASE, state.diskEtag ?? 'e9'))
    }
    if (method === 'PUT') return Promise.resolve(state.puts?.shift() ?? putRes())
    return Promise.reject(new Error(`unexpected ${method}`))
  })
}

function putCalls() {
  return fetchMock.mock.calls.filter(([, init]) => init?.method === 'PUT')
}
function lastPutBody(): { content: string } {
  const calls = putCalls()
  return JSON.parse(calls[calls.length - 1]![1]!.body as string)
}

let queryClient: QueryClient
function wrapper({ children }: { children: ReactNode }) {
  return <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
}
/** Seed the shared cache the hook writes through (as useDesignerDoc would). */
function seed(content = BASE, etag = 'e1') {
  queryClient.setQueryData(['file-content', PATH], { content, etag })
}
function render(readOnly = false) {
  return renderHook(() => useDesignerWrite(PATH, FLOWGROUP, readOnly), { wrapper })
}

beforeEach(() => {
  vi.clearAllMocks()
  vi.stubGlobal('fetch', fetchMock)
  queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } })
})
afterEach(() => vi.unstubAllGlobals())

describe('useDesignerWrite', () => {
  it('commit set: PUTs the serialized body with the If-Match etag', async () => {
    serve({})
    seed()
    const { result } = render()

    act(() => {
      result.current.commit((doc) => setActionField(doc, 'a1', ['target'], 'v_final'))
    })

    await waitFor(() => expect(putCalls().length).toBe(1))
    const [url, init] = putCalls()[0]!
    expect(url).toBe('/api/files/pipelines/x.yaml')
    expect((init!.headers as Record<string, string>)['If-Match']).toBe('"e1"')
    expect(lastPutBody().content).toContain('target: v_final')
    // Surgical: the untouched sql key survives.
    expect(lastPutBody().content).toContain('sql: SELECT 1')
    // The cache advanced to the new etag so the next edit chains cleanly.
    await waitFor(() =>
      expect(
        queryClient.getQueryData<{ etag: string }>(['file-content', PATH])?.etag,
      ).toBe('e2'),
    )
  })

  it('delete-on-clear: committing deleteActionField removes the key', async () => {
    serve({})
    seed()
    const { result } = render()

    act(() => {
      result.current.commit((doc) => deleteActionField(doc, 'a1', ['sql']))
    })

    await waitFor(() => expect(putCalls().length).toBe(1))
    expect(lastPutBody().content).not.toContain('sql:')
    expect(lastPutBody().content).toContain('target: v_out')
  })

  it('a no-op mutation writes nothing', async () => {
    serve({})
    seed()
    const { result } = render()

    act(() => {
      // Setting the same value produces byte-identical YAML → no PUT.
      result.current.commit((doc) => setActionField(doc, 'a1', ['target'], 'v_out'))
    })
    await new Promise((r) => setTimeout(r, 20))
    expect(putCalls().length).toBe(0)
  })

  it('dirty-buffer guard hard-blocks the write path', async () => {
    serve({})
    seed()
    const { result } = render(true) // readOnly

    act(() => {
      result.current.commit((doc) => setActionField(doc, 'a1', ['target'], 'v_x'))
    })
    await new Promise((r) => setTimeout(r, 20))
    expect(fetchMock).not.toHaveBeenCalled()
  })

  it('serializes in-flight commits: the second applies on top of the first', async () => {
    serve({ puts: [putRes('e2'), putRes('e3')] })
    seed()
    const { result } = render()

    act(() => {
      result.current.commit((doc) => setActionField(doc, 'a1', ['target'], 'v_A'))
      result.current.commit((doc) => setActionField(doc, 'a1', ['description'], 'B'))
    })

    await waitFor(() => expect(putCalls().length).toBe(2))
    // Second PUT used the first PUT's returned etag …
    expect((putCalls()[1]![1]!.headers as Record<string, string>)['If-Match']).toBe('"e2"')
    // … and its body carries BOTH edits (proves no lost update).
    const body = lastPutBody().content
    expect(body).toContain('target: v_A')
    expect(body).toContain('description: B')
  })

  it('rename: persists and resolves true', async () => {
    serve({})
    seed()
    const { result } = render()

    let ok: boolean | undefined
    await act(async () => {
      ok = await result.current.rename('a1', 'a1_renamed')
    })
    expect(ok).toBe(true)
    expect(lastPutBody().content).toContain('name: a1_renamed')
  })

  it('rename: read-only resolves false and writes nothing', async () => {
    serve({})
    seed()
    const { result } = render(true)

    let ok: boolean | undefined
    await act(async () => {
      ok = await result.current.rename('a1', 'a1_renamed')
    })
    expect(ok).toBe(false)
    expect(fetchMock).not.toHaveBeenCalled()
  })

  it('412 raises conflict; overwrite re-PUTs with a fresh etag', async () => {
    serve({ diskEtag: 'e-fresh', puts: [staleRes(), putRes('e2')] })
    seed()
    const { result } = render()

    act(() => {
      result.current.commit((doc) => setActionField(doc, 'a1', ['target'], 'v_C'))
    })
    await waitFor(() => expect(result.current.conflict).toBe(true))

    await act(async () => {
      await result.current.overwrite()
    })
    await waitFor(() => expect(result.current.conflict).toBe(false))

    // Overwrite fetched a fresh etag then re-PUT the same edit.
    expect((putCalls()[1]![1]!.headers as Record<string, string>)['If-Match']).toBe('"e-fresh"')
    expect(lastPutBody().content).toContain('target: v_C')
  })
})
