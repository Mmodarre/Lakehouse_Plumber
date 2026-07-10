/* eslint-disable react-refresh/only-export-components -- test-support
   module (helpers + a private harness component); fast refresh never
   applies to test files. */
import { vi } from 'vitest'
import type { ReactNode } from 'react'
import { render } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { useConfigFile } from '../../../../hooks/useConfigFile'
import { serveConfigFile } from '../../shared/__tests__/configFormTestSupport'
import { JobConfigEditor } from '../JobConfigEditor'

export { installRadixStubs } from '../../shared/__tests__/configFormTestSupport'

// ── Shared harness for the JobConfigEditor suites ────────────
//
// Same discipline as the pipeline harness: the REAL stack (useConfigFile
// → yaml-doc → editor) over the shared stubbed fetch, so byte-preservation
// assertions run against the actual PUT body. Tests await their own mode
// marker (rail nav / banner / settings heading) — the three file shapes
// render different roots.

export const JOB_CONFIG_PATH = 'config/job_config.yaml'
export const MONITORING_JOB_CONFIG_PATH = 'config/monitoring_job_config.yaml'

export const fetchMock =
  vi.fn<(url: string | URL | Request, init?: RequestInit) => Promise<Response>>()

/** Serve GET/PUT for a job config at `path` (default: standard job_config). */
export function serveJob(
  content: string,
  { path = JOB_CONFIG_PATH }: { path?: string } = {},
): { putBodies: () => string[] } {
  return serveConfigFile(fetchMock, path, content)
}

function Harness({ path }: { path: string }) {
  const file = useConfigFile(path)
  return <JobConfigEditor file={file} />
}

/** Mount the editor; callers await their own mode-specific marker. */
export function renderJobEditor(path: string = JOB_CONFIG_PATH): void {
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } })
  const wrapper = ({ children }: { children: ReactNode }) => (
    <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
  )
  render(<Harness path={path} />, { wrapper })
}
