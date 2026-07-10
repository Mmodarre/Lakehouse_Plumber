/* eslint-disable react-refresh/only-export-components -- test-support
   module (helpers + a private harness component); fast refresh never
   applies to test files. */
import { expect, vi } from 'vitest'
import type { ReactNode } from 'react'
import { render, screen, waitFor } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { useConfigFile } from '../../../../hooks/useConfigFile'
import { serveConfigFile } from '../../shared/__tests__/configFormTestSupport'
import { PipelineConfigEditor } from '../PipelineConfigEditor'

export { installRadixStubs } from '../../shared/__tests__/configFormTestSupport'

// ── Shared harness for the PipelineConfigEditor suites ───────
//
// Same discipline as projectFormTestSupport: the REAL stack
// (useConfigFile → yaml-doc → editor) over a stubbed global fetch (the
// generic server lives in shared/__tests__/configFormTestSupport), so
// byte-preservation assertions run against the actual PUT body.

export const PIPELINE_CONFIG_PATH = 'config/pipeline_config.yaml'

export const fetchMock =
  vi.fn<(url: string | URL | Request, init?: RequestInit) => Promise<Response>>()

/**
 * Serve GET of the pipeline config from `content`, record PUT bodies, and
 * answer /api/pipelines with `pipelines` (GroupMembershipEditor
 * suggestions).
 */
export function servePipeline(
  content: string,
  { pipelines = [] as string[] } = {},
): { putBodies: () => string[] } {
  return serveConfigFile(fetchMock, PIPELINE_CONFIG_PATH, content, {
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
}

function Harness() {
  const file = useConfigFile(PIPELINE_CONFIG_PATH)
  return <PipelineConfigEditor file={file} />
}

export async function renderPipelineEditor(): Promise<void> {
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } })
  const wrapper = ({ children }: { children: ReactNode }) => (
    <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
  )
  render(<Harness />, { wrapper })
  // The rail renders once the file is loaded and parsed (every fixture in
  // these suites parses cleanly).
  await waitFor(() =>
    expect(
      screen.getByRole('navigation', { name: 'Configuration documents' }),
    ).toBeInTheDocument(),
  )
}
