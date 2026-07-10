import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import type { ReactNode } from 'react'
import { act, renderHook, waitFor } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'

vi.mock('sonner', () => ({
  toast: { error: vi.fn(), success: vi.fn(), dismiss: vi.fn() },
}))

import { setPath, toJS } from '../../lib/yaml-doc'
import { useConfigFile } from '../useConfigFile'

// ── fetch-level mocking ──────────────────────────────────────
//
// The suite stubs global fetch (below the real api/client + api/files
// modules) so the assertions cover the actual wire contract: the If-Match
// header the PUT carries and the serialized YAML body, not a mocked
// helper's arguments.

const fetchMock = vi.fn<(url: string, init?: RequestInit) => Promise<Response>>()

function textRes(content: string, etag: string): Response {
  return new Response(content, { status: 200, headers: { ETag: `"${etag}"` } })
}

function putRes(overrides: Record<string, unknown> = {}): Response {
  return new Response(
    JSON.stringify({
      written: true,
      path: 'lhp.yaml',
      yaml_error: null,
      etag: 'e2',
      ...overrides,
    }),
    { status: 200 },
  )
}

function staleRes(): Response {
  return new Response(JSON.stringify({ detail: 'File was modified since you last read it.' }), {
    status: 412,
  })
}

/** Serve `GET` from a mutable ref and `PUT` from a queue (default success). */
function serve(state: { content: string; etag: string; putResponses?: Response[] }) {
  fetchMock.mockImplementation((url, init) => {
    const method = init?.method ?? 'GET'
    if (method === 'GET') return Promise.resolve(textRes(state.content, state.etag))
    if (method === 'PUT') {
      const next = state.putResponses?.shift() ?? putRes()
      return Promise.resolve(next)
    }
    return Promise.reject(new Error(`unexpected ${method} ${String(url)}`))
  })
}

function putCalls() {
  return fetchMock.mock.calls.filter(([, init]) => init?.method === 'PUT')
}

function getCalls() {
  return fetchMock.mock.calls.filter(([, init]) => (init?.method ?? 'GET') === 'GET')
}

let queryClient: QueryClient

function wrapper({ children }: { children: ReactNode }) {
  return <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
}

async function renderLoaded(path: string | null = 'lhp.yaml') {
  const rendered = renderHook(({ p }: { p: string | null }) => useConfigFile(p), {
    wrapper,
    initialProps: { p: path },
  })
  if (path !== null) {
    await waitFor(() => expect(rendered.result.current.handle).not.toBeNull())
  }
  return rendered
}

beforeEach(() => {
  vi.clearAllMocks()
  vi.stubGlobal('fetch', fetchMock)
  queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } })
})

afterEach(() => {
  vi.unstubAllGlobals()
})

describe('useConfigFile', () => {
  it('null path: fetches nothing, exposes empty state, save() is a no-op', async () => {
    const { result } = await renderLoaded(null)

    expect(fetchMock).not.toHaveBeenCalled()
    expect(result.current.handle).toBeNull()
    expect(result.current.dirty).toBe(false)
    expect(result.current.errors).toEqual([])
    expect(result.current.isLoading).toBe(false)

    let ok = true
    await act(async () => {
      ok = await result.current.save()
    })
    expect(ok).toBe(false)
    expect(fetchMock).not.toHaveBeenCalled()
  })

  it('load → mutate → dirty → save PUTs the serialized body with the If-Match header', async () => {
    serve({ content: 'name: test\ncount: 1\n', etag: 'e1' })
    const { result } = await renderLoaded()

    expect(getCalls()[0]![0]).toBe('/api/files/lhp.yaml')
    expect(result.current.etag).toBe('e1')
    expect(result.current.errors).toEqual([])
    expect(toJS(result.current.handle!, 0)).toEqual({ name: 'test', count: 1 })

    const versionBefore = result.current.version
    act(() => {
      result.current.mutate((h) => setPath(h, 0, ['count'], 2))
    })
    expect(result.current.dirty).toBe(true)
    expect(result.current.version).toBeGreaterThan(versionBefore)

    let ok = false
    await act(async () => {
      ok = await result.current.save()
    })
    expect(ok).toBe(true)

    const [url, init] = putCalls()[0]!
    expect(url).toBe('/api/files/lhp.yaml')
    expect((init!.headers as Record<string, string>)['If-Match']).toBe('"e1"')
    expect(JSON.parse(init!.body as string)).toEqual({ content: 'name: test\ncount: 2\n' })

    // Success adopts the write: etag advances, dirty clears, handle survives
    // (re-parsed from the serialized text so later edits stay byte-surgical).
    expect(result.current.dirty).toBe(false)
    expect(result.current.etag).toBe('e2')
    expect(toJS(result.current.handle!, 0)).toEqual({ name: 'test', count: 2 })
    expect(result.current.conflict).toBe(false)
  })

  it('412 → conflict; overwrite() re-GETs for a fresh etag and re-PUTs my content', async () => {
    const state = { content: 'name: test\n', etag: 'e1', putResponses: [staleRes()] }
    serve(state)
    const { result } = await renderLoaded()

    act(() => {
      result.current.mutate((h) => setPath(h, 0, ['name'], 'mine'))
    })
    let ok = true
    await act(async () => {
      ok = await result.current.save()
    })
    expect(ok).toBe(false)
    expect(result.current.conflict).toBe(true)
    expect(result.current.dirty).toBe(true)

    // Cancel clears the flag without resolving; the edits stay dirty.
    act(() => {
      result.current.dismissConflict()
    })
    expect(result.current.conflict).toBe(false)
    expect(result.current.dirty).toBe(true)

    // Disk moved on: overwrite must fetch the FRESH etag, never reuse e1.
    state.content = 'name: theirs\n'
    state.etag = 'e-disk'
    state.putResponses = [putRes({ etag: 'e3' })]
    const getsBefore = getCalls().length

    await act(async () => {
      ok = await result.current.overwrite()
    })
    expect(ok).toBe(true)
    expect(getCalls().length).toBeGreaterThan(getsBefore)

    const [, init] = putCalls().at(-1)!
    expect((init!.headers as Record<string, string>)['If-Match']).toBe('"e-disk"')
    expect(JSON.parse(init!.body as string)).toEqual({ content: 'name: mine\n' })
    expect(result.current.conflict).toBe(false)
    expect(result.current.dirty).toBe(false)
    expect(result.current.etag).toBe('e3')
  })

  it('a 412 for a save that targeted a PREVIOUS path never flags conflict on the new path', async () => {
    // Deferred PUT: the save is still in flight when the hook switches to
    // another file; the late 412 belongs to the old path and must not put
    // the new path's state into conflict.
    let resolvePut: (res: Response) => void
    const putGate = new Promise<Response>((resolve) => {
      resolvePut = resolve
    })
    const contents: Record<string, { content: string; etag: string }> = {
      'lhp.yaml': { content: 'name: a\n', etag: 'e1' },
      'config/other.yaml': { content: 'name: b\n', etag: 'e2' },
    }
    fetchMock.mockImplementation((url, init) => {
      const method = init?.method ?? 'GET'
      if (method === 'PUT') return putGate
      const path = String(url).replace('/api/files/', '')
      const file = contents[path]!
      return Promise.resolve(textRes(file.content, file.etag))
    })

    const { result, rerender } = await renderLoaded()

    act(() => {
      result.current.mutate((h) => setPath(h, 0, ['name'], 'mine'))
    })
    let savePromise!: Promise<boolean>
    act(() => {
      savePromise = result.current.save()
    })

    // Switch files while the PUT is in flight, then let it fail with 412.
    rerender({ p: 'config/other.yaml' })
    await waitFor(() => expect(result.current.etag).toBe('e2'))
    await act(async () => {
      resolvePut(staleRes())
      await savePromise
    })

    expect(result.current.conflict).toBe(false)
    expect(result.current.dirty).toBe(false)
  })

  it('412 → reload() discards local edits and adopts the disk version', async () => {
    const state = { content: 'name: test\n', etag: 'e1', putResponses: [staleRes()] }
    serve(state)
    const { result } = await renderLoaded()

    act(() => {
      result.current.mutate((h) => setPath(h, 0, ['name'], 'mine'))
    })
    await act(async () => {
      await result.current.save()
    })
    expect(result.current.conflict).toBe(true)

    state.content = 'name: disk\n'
    state.etag = 'e-disk'
    await act(async () => {
      await result.current.reload()
    })

    expect(toJS(result.current.handle!, 0)).toEqual({ name: 'disk' })
    expect(result.current.dirty).toBe(false)
    expect(result.current.conflict).toBe(false)
    expect(result.current.etag).toBe('e-disk')
    expect(putCalls()).toHaveLength(1) // reload never writes
  })

  it('external change while DIRTY sets the flag and keeps my edits; keepMine dismisses it', async () => {
    const state = { content: 'name: test\n', etag: 'e1' }
    serve(state)
    const { result } = await renderLoaded()

    act(() => {
      result.current.mutate((h) => setPath(h, 0, ['name'], 'mine'))
    })

    // SSE reports a change → usePushChannel invalidates ['file-content', path].
    state.content = 'name: external\n'
    state.etag = 'e9'
    await act(async () => {
      await queryClient.invalidateQueries({ queryKey: ['file-content', 'lhp.yaml'] })
    })

    await waitFor(() => expect(result.current.externalChange).toBe(true))
    expect(toJS(result.current.handle!, 0)).toEqual({ name: 'mine' }) // not clobbered
    expect(result.current.dirty).toBe(true)

    act(() => {
      result.current.keepMine()
    })
    expect(result.current.externalChange).toBe(false)
    expect(result.current.dirty).toBe(true)
  })

  it('external change while CLEAN silently re-parses the new content', async () => {
    const state = { content: 'name: test\n', etag: 'e1' }
    serve(state)
    const { result } = await renderLoaded()

    state.content = 'name: external\n'
    state.etag = 'e9'
    await act(async () => {
      await queryClient.invalidateQueries({ queryKey: ['file-content', 'lhp.yaml'] })
    })

    await waitFor(() => expect(result.current.etag).toBe('e9'))
    expect(toJS(result.current.handle!, 0)).toEqual({ name: 'external' })
    expect(result.current.externalChange).toBe(false)
    expect(result.current.dirty).toBe(false)
  })

  it('surfaces yaml_error from the write response (write persisted)', async () => {
    serve({
      content: 'name: test\n',
      etag: 'e1',
      putResponses: [putRes({ yaml_error: { line: 1, column: 2, message: 'bad yaml' } })],
    })
    const { result } = await renderLoaded()

    act(() => {
      result.current.mutate((h) => setPath(h, 0, ['name'], 'x'))
    })
    let ok = false
    await act(async () => {
      ok = await result.current.save()
    })

    expect(ok).toBe(true)
    expect(result.current.yamlError).toEqual({ line: 1, column: 2, message: 'bad yaml' })
    expect(result.current.dirty).toBe(false)
  })

  it('a file with parse errors loads (errors exposed) but blocks mutate and save', async () => {
    serve({ content: 'foo: [unclosed\n', etag: 'e1' })
    const { result } = await renderLoaded()

    expect(result.current.errors.length).toBeGreaterThan(0)

    expect(() => {
      result.current.mutate((h) => setPath(h, 0, ['foo'], 1))
    }).toThrow(/parse error/)

    let ok = true
    await act(async () => {
      ok = await result.current.save()
    })
    expect(ok).toBe(false)
    expect(putCalls()).toHaveLength(0)
  })

  it('exposes a load error when the GET fails', async () => {
    fetchMock.mockImplementation(() =>
      Promise.resolve(new Response(JSON.stringify({ detail: 'File not found: lhp.yaml' }), { status: 404 })),
    )
    const { result } = renderHook(() => useConfigFile('lhp.yaml'), { wrapper })

    await waitFor(() => expect(result.current.loadError).not.toBeNull())
    expect(result.current.loadError).toContain('File not found')
    expect(result.current.handle).toBeNull()
  })
})
