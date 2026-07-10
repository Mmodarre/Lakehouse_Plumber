import { vi } from 'vitest'

// ── Shared fetch-stub harness for the config editor suites ───
//
// Extracted from pipelineFormTestSupport (Task 9) so the pipeline and job
// editor suites share one wire-level discipline: the REAL stack
// (useConfigFile → yaml-doc → editor) over a stubbed global fetch, with
// byte-preservation assertions running against the actual PUT body.

export type FetchMock = ReturnType<
  typeof vi.fn<(url: string | URL | Request, init?: RequestInit) => Promise<Response>>
>

/**
 * Wire `fetchMock` to serve GET/PUT for one config file at `path` from
 * `content`, recording PUT bodies. `extraGet` maps additional GET urls to
 * response factories (e.g. /api/pipelines suggestions).
 */
export function serveConfigFile(
  fetchMock: FetchMock,
  path: string,
  content: string,
  extraGet: Record<string, () => Response> = {},
): { putBodies: () => string[] } {
  let etagSeq = 0
  fetchMock.mockImplementation((url, init) => {
    const u = String(url)
    const method = init?.method ?? 'GET'
    if (method === 'GET' && u in extraGet) {
      return Promise.resolve(extraGet[u]())
    }
    if (method === 'GET' && u === `/api/files/${path}`) {
      return Promise.resolve(
        new Response(content, { status: 200, headers: { ETag: `"e${etagSeq}"` } }),
      )
    }
    if (method === 'PUT' && u === `/api/files/${path}`) {
      etagSeq += 1
      return Promise.resolve(
        new Response(
          JSON.stringify({
            written: true,
            path,
            yaml_error: null,
            etag: `e${etagSeq}`,
          }),
          { status: 200 },
        ),
      )
    }
    return Promise.reject(new Error(`unexpected ${method} ${u}`))
  })
  return {
    putBodies: () =>
      fetchMock.mock.calls
        .filter(([, init]) => init?.method === 'PUT')
        .map(([, init]) => (JSON.parse(init!.body as string) as { content: string }).content),
  }
}

/** Radix Select/Popover + cmdk need layout APIs jsdom lacks. */
export function installRadixStubs(): void {
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
}
