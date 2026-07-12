import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import type { ReactNode } from 'react'
import { act, renderHook, waitFor } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'

vi.mock('sonner', () => ({
  toast: { error: vi.fn(), success: vi.fn(), dismiss: vi.fn() },
}))

import { addTemplateParam, setTemplateParamField } from '@/lib/flowgroup-doc'
import { useDesignerWrite } from '../useDesignerWrite'

// Template write-through: docKind='template' selects the template body, so
// parameter mutators PUT the serialized `parameters:` block. The enforcing
// dirty guard (readOnly) hard-blocks the write, exactly as for flowgroups.

const PATH = 'templates/csv_ingestion_template.yaml'
const NAME = 'csv_ingestion_template'
const BASE = `name: csv_ingestion_template
parameters:
  - name: table_name
    required: true
actions:
  - name: load_{{ table_name }}
    type: load
    source:
      type: cloudfiles
      path: "/land/{{ table_name }}"
      format: csv
    target: v_{{ table_name }}
`

const fetchMock = vi.fn<(url: string, init?: RequestInit) => Promise<Response>>()

function putRes(etag = 'e2'): Response {
  return new Response(JSON.stringify({ written: true, path: PATH, yaml_error: null, etag }), {
    status: 200,
  })
}
function serve() {
  fetchMock.mockImplementation((_url, init) => {
    const method = init?.method ?? 'GET'
    if (method === 'GET') {
      return Promise.resolve(new Response(BASE, { status: 200, headers: { ETag: '"e9"' } }))
    }
    if (method === 'PUT') return Promise.resolve(putRes())
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
function seed(content = BASE, etag = 'e1') {
  queryClient.setQueryData(['file-content', PATH], { content, etag })
}
function render(readOnly = false) {
  return renderHook(() => useDesignerWrite(PATH, NAME, readOnly, 'template'), { wrapper })
}

beforeEach(() => {
  vi.clearAllMocks()
  vi.stubGlobal('fetch', fetchMock)
  queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } })
})
afterEach(() => vi.unstubAllGlobals())

describe('useDesignerWrite — template mode', () => {
  it('commits a new parameter to the template parameters block', async () => {
    serve()
    seed()
    const { result } = render()

    act(() => {
      result.current.commit((body) => addTemplateParam(body, { name: 'landing_folder' }))
    })

    await waitFor(() => expect(putCalls().length).toBe(1))
    const body = lastPutBody().content
    expect(body).toContain('name: landing_folder')
    // The action body (with its {{ }} tokens) is untouched.
    expect(body).toContain('target: v_{{ table_name }}')
  })

  it('commits a parameter field edit surgically', async () => {
    serve()
    seed()
    const { result } = render()

    act(() => {
      result.current.commit((body) => setTemplateParamField(body, 0, ['type'], 'string'))
    })

    await waitFor(() => expect(putCalls().length).toBe(1))
    expect(lastPutBody().content).toContain('type: string')
  })

  it('dirty-buffer guard hard-blocks parameter edits', async () => {
    serve()
    seed()
    const { result } = render(true) // readOnly

    act(() => {
      result.current.commit((body) => addTemplateParam(body, { name: 'blocked' }))
    })
    await new Promise((r) => setTimeout(r, 20))
    expect(fetchMock).not.toHaveBeenCalled()
  })
})
