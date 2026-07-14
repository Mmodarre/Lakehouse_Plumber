import { vi } from 'vitest'
import {
  configureFetch,
  mountConfigFormView,
  resetConfigStores,
  seedConfigBuffer,
} from '../../shared/__tests__/configFormTestSupport'
import type { FetchMock } from '../../shared/__tests__/configFormTestSupport'

export { installRadixStubs } from '../../shared/__tests__/configFormTestSupport'

// ── Shared harness for the ProjectConfigForm suites ──────────
//
// Seeds a workspace buffer with the fixture, then renders the project Form
// view (ConfigFormView → ProjectConfigForm bound to documentStore). Editing
// pushes byte-surgical serialized text into the buffer, so byte-preservation
// assertions run against `bufferContent()` (what a ⌘S would write) — the
// model has no PUT.

export const PROJECT_CONFIG_PATH = 'lhp.yaml'

export const fetchMock: FetchMock =
  vi.fn<(url: string | URL | Request, init?: RequestInit) => Promise<Response>>()

/** Reset the stores and seed lhp.yaml from `content`; returns
 * `bufferContent()` — the serialized buffer text after edits. */
export function serveProject(content: string): { bufferContent: () => string } {
  resetConfigStores()
  configureFetch(fetchMock)
  return seedConfigBuffer(PROJECT_CONFIG_PATH, content)
}

export function renderProjectForm(): void {
  // The (i) help tooltip's Radix Arrow measures via ResizeObserver, which
  // jsdom lacks — install the same stub the pipeline/job harnesses use.
  vi.stubGlobal(
    'ResizeObserver',
    class {
      observe() {}
      unobserve() {}
      disconnect() {}
    },
  )
  mountConfigFormView(PROJECT_CONFIG_PATH, 'project')
}

/** Zip-compare two texts line by line; returns the differing line pairs. */
export function lineDiff(
  before: string,
  after: string,
): { index: number; before: string | undefined; after: string | undefined }[] {
  const a = before.split('\n')
  const b = after.split('\n')
  const out: { index: number; before: string | undefined; after: string | undefined }[] = []
  for (let i = 0; i < Math.max(a.length, b.length); i++) {
    if (a[i] !== b[i]) out.push({ index: i, before: a[i], after: b[i] })
  }
  return out
}
