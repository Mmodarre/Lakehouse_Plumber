import { expect, vi } from 'vitest'
import { screen, waitFor } from '@testing-library/react'
import {
  configureFetch,
  mountConfigFormView,
  resetConfigStores,
  seedConfigBuffer,
} from '../../shared/__tests__/configFormTestSupport'
import type { FetchMock } from '../../shared/__tests__/configFormTestSupport'

export { installRadixStubs } from '../../shared/__tests__/configFormTestSupport'

// ── Shared harness for the PipelineConfigEditor suites ───────
//
// Seeds a workspace buffer with the fixture, then renders the pipeline Form
// view (ConfigFormView → PipelineConfigEditor bound to documentStore).
// Editing pushes byte-surgical serialized text into the buffer, so
// byte-preservation assertions run against `bufferContent()` (what a ⌘S
// would write) — the model has no PUT.

export const PIPELINE_CONFIG_PATH = 'config/pipeline_config.yaml'

export const fetchMock: FetchMock =
  vi.fn<(url: string | URL | Request, init?: RequestInit) => Promise<Response>>()

/**
 * Reset the stores, seed the pipeline config from `content`, and answer
 * /api/pipelines with `pipelines` (GroupMembershipEditor suggestions).
 * Returns `bufferContent()` — the serialized buffer text after edits.
 */
export function servePipeline(
  content: string,
  { pipelines = [] as string[] } = {},
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
  return seedConfigBuffer(PIPELINE_CONFIG_PATH, content)
}

export async function renderPipelineEditor(): Promise<void> {
  mountConfigFormView(PIPELINE_CONFIG_PATH, 'pipeline')
  // The rail renders once the buffer is parsed (every fixture in these
  // suites parses cleanly).
  await waitFor(() =>
    expect(
      screen.getByRole('navigation', { name: 'Configuration documents' }),
    ).toBeInTheDocument(),
  )
}
