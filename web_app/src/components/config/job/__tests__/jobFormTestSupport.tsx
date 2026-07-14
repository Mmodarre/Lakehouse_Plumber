import { vi } from 'vitest'
import {
  configureFetch,
  mountConfigFormView,
  resetConfigStores,
  seedConfigBuffer,
} from '../../shared/__tests__/configFormTestSupport'
import type { FetchMock } from '../../shared/__tests__/configFormTestSupport'

export { installRadixStubs } from '../../shared/__tests__/configFormTestSupport'

// ── Shared harness for the JobConfigEditor suites ────────────
//
// Same discipline as the pipeline harness: seed a workspace buffer with the
// fixture, render the job Form view (ConfigFormView → JobConfigEditor bound
// to documentStore). The three file shapes render different roots, so tests
// await their own mode marker (rail nav / banner / settings heading).
// Byte-preservation runs against `bufferContent()` (what a ⌘S would write).

export const JOB_CONFIG_PATH = 'config/job_config.yaml'
export const MONITORING_JOB_CONFIG_PATH = 'config/monitoring_job_config.yaml'

export const fetchMock: FetchMock =
  vi.fn<(url: string | URL | Request, init?: RequestInit) => Promise<Response>>()

/**
 * Reset the stores and seed a job config from `content` (default: standard
 * job_config). `pipelines` answers /api/pipelines (master-job suggestions).
 * Returns `bufferContent()` — the serialized buffer text after edits.
 */
export function serveJob(
  content: string,
  {
    path = JOB_CONFIG_PATH,
    pipelines = [] as string[],
  }: { path?: string; pipelines?: string[] } = {},
): { bufferContent: () => string } {
  resetConfigStores()
  configureFetch(fetchMock, {
    '/api/pipelines': () =>
      new Response(
        JSON.stringify({
          pipelines: pipelines.map((name) => ({
            name,
            flowgroup_count: 1,
            action_count: 1,
          })),
          total: pipelines.length,
        }),
        { status: 200 },
      ),
  })
  return seedConfigBuffer(path, content)
}

/** Mount the editor; callers await their own mode-specific marker. */
export function renderJobEditor(path: string = JOB_CONFIG_PATH): void {
  mountConfigFormView(path, 'job')
}
