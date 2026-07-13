/* eslint-disable react-refresh/only-export-components -- test-support
   module (helpers + a private harness component); fast refresh never
   applies to test files. */
import { expect, vi } from 'vitest'
import type { ReactNode } from 'react'
import { render, waitFor, screen } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { TooltipProvider } from '@/components/ui/tooltip'
import { useConfigFile } from '../../../../hooks/useConfigFile'
import { ProjectConfigForm } from '../ProjectConfigForm'

// ── Shared harness for the ProjectConfigForm suites ──────────
//
// Renders the REAL stack (useConfigFile → yaml-doc → form) over a stubbed
// global fetch, so byte-preservation assertions run against the actual PUT
// body the form would send — not a mocked helper's arguments.

export const fetchMock =
  vi.fn<(url: string | URL | Request, init?: RequestInit) => Promise<Response>>()

export function textRes(content: string, etag = 'e1'): Response {
  return new Response(content, { status: 200, headers: { ETag: `"${etag}"` } })
}

export function putRes(overrides: Record<string, unknown> = {}): Response {
  return new Response(
    JSON.stringify({ written: true, path: 'lhp.yaml', yaml_error: null, etag: 'e2', ...overrides }),
    { status: 200 },
  )
}

/** Serve GET from `content`; record PUT bodies (always succeeding). */
export function serveProject(content: string): { putBodies: () => string[] } {
  let etagSeq = 0
  fetchMock.mockImplementation((url, init) => {
    const method = init?.method ?? 'GET'
    if (method === 'GET') return Promise.resolve(textRes(content, `e${etagSeq}`))
    if (method === 'PUT') {
      etagSeq += 1
      return Promise.resolve(putRes({ etag: `e${etagSeq}` }))
    }
    return Promise.reject(new Error(`unexpected ${method} ${String(url)}`))
  })
  return {
    putBodies: () =>
      fetchMock.mock.calls
        .filter(([, init]) => init?.method === 'PUT')
        .map(([, init]) => (JSON.parse(init!.body as string) as { content: string }).content),
  }
}

function Harness() {
  const file = useConfigFile('lhp.yaml')
  return <ProjectConfigForm file={file} />
}

export async function renderProjectForm(): Promise<void> {
  // The Radix tooltip Arrow (now rendered by FieldLabel's (i) help icon)
  // measures itself via ResizeObserver, which jsdom lacks — same stub the
  // pipeline/job harnesses install via installRadixStubs.
  vi.stubGlobal(
    'ResizeObserver',
    class {
      observe() {}
      unobserve() {}
      disconnect() {}
    },
  )
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } })
  // The form's SchemaKindProvider calls useQuery(['schema','project']);
  // seed it so no real GET /api/schemas fires (staleTime: Infinity → fresh).
  queryClient.setQueryData(['schema', 'project'], {})
  const wrapper = ({ children }: { children: ReactNode }) => (
    <QueryClientProvider client={queryClient}>
      <TooltipProvider delayDuration={0}>{children}</TooltipProvider>
    </QueryClientProvider>
  )
  render(<Harness />, { wrapper })
  // The SaveBar is always present; the form body appears once loaded —
  // tests wait for their own fields after this.
  await waitFor(() => expect(screen.getByRole('button', { name: 'Save' })).toBeInTheDocument())
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
