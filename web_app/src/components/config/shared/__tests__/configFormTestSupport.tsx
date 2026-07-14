import { vi } from 'vitest'
import type { ReactNode } from 'react'
import { render } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { TooltipProvider } from '@/components/ui/tooltip'
import { ConfigFormView } from '../../../entity/ConfigFormView'
import type { ConfigKind } from '../../../../store/workspaceStore'
import { useWorkspaceStore } from '../../../../store/workspaceStore'
import { useDocumentStore } from '../../../../store/documentStore'
import { useLayoutStore } from '../../../../store/layoutStore'

// ── Shared harness for the config Form-view suites ───────────
//
// The config editors used to run the round-trip through useConfigFile →
// yaml-doc → editor over a stubbed GET/PUT. That lifecycle is gone: the
// forms now bind to the documentStore, editing pushes byte-surgical
// serialized text straight into the workspace buffer (there is no PUT —
// saving is the buffer's ⌘S path), and degraded/viewer state comes from
// useEntityDocument. So the harness seeds a workspace buffer with the
// fixture content, renders ConfigFormView (the real re-host), and exposes
// `bufferContent()` — the serialized text after each edit, i.e. exactly what
// a save would write. Byte-preservation assertions run against THAT (they
// used to run against the PUT body).

export type FetchMock = ReturnType<
  typeof vi.fn<(url: string | URL | Request, init?: RequestInit) => Promise<Response>>
>

const SCHEMA_KIND: Record<ConfigKind, string> = {
  project: 'project',
  pipeline: 'pipeline_config',
  job: 'job_config',
}

/** Reset every store ConfigFormView reads, so suites never leak into each
 * other (documentStore handles, workspace buffers, the viewer lens). */
export function resetConfigStores(): void {
  useDocumentStore.setState({ docs: {} })
  useWorkspaceStore.setState({
    buffers: [],
    tabs: [],
    activePath: null,
    projectRoot: null,
    restoredDirtyCount: 0,
  })
  useLayoutStore.setState({ viewerMode: false })
}

/** Seed a workspace buffer at `path` with `content`; returns the live
 * serialized-text accessor (what a ⌘S would write after edits). */
export function seedConfigBuffer(path: string, content: string): { bufferContent: () => string } {
  useWorkspaceStore.getState().openBuffer(path, {
    content,
    originalContent: content,
    exists: true,
    etag: 'e0',
  })
  return {
    bufferContent: () =>
      useWorkspaceStore.getState().buffers.find((b) => b.path === path)?.content ?? '',
  }
}

/** Point `fetchMock` at `extraGet` suggestion endpoints (e.g. /api/pipelines);
 * every other request rejects (there are no file GET/PUT calls now). */
export function configureFetch(
  fetchMock: FetchMock,
  extraGet: Record<string, () => Response> = {},
): void {
  fetchMock.mockImplementation((url, init) => {
    const u = String(url)
    if ((init?.method ?? 'GET') === 'GET' && u in extraGet) {
      return Promise.resolve(extraGet[u]())
    }
    return Promise.reject(new Error(`unexpected ${init?.method ?? 'GET'} ${u}`))
  })
}

/** Mount ConfigFormView for `configKind` at `path` (the buffer must already
 * be seeded). Seeds the schema query so no /api/schemas GET fires. Synchronous
 * — the documentStore handle parses in ConfigFormView's mount effect, so
 * callers await their own readiness marker (rail nav / field / banner). */
export function mountConfigFormView(path: string, configKind: ConfigKind): void {
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } })
  queryClient.setQueryData(['schema', SCHEMA_KIND[configKind]], {})
  const wrapper = ({ children }: { children: ReactNode }) => (
    <QueryClientProvider client={queryClient}>
      <TooltipProvider delayDuration={0}>{children}</TooltipProvider>
    </QueryClientProvider>
  )
  render(<ConfigFormView tabId={`config:${path}`} path={path} configKind={configKind} />, {
    wrapper,
  })
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
