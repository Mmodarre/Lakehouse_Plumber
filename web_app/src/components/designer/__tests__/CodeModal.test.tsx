import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import type { ReactNode } from 'react'
import { fireEvent, render, screen, waitFor } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'

vi.mock('sonner', () => ({ toast: { error: vi.fn(), success: vi.fn() } }))

// Stub Monaco: a textarea that mirrors the editor's value through the ref's
// getValue(), so the modal's Save reads what was typed without real Monaco.
vi.mock('../../editor/MonacoEditorWrapper', async () => {
  const react = await import('react')
  return {
    default: react.forwardRef(function MockEditor(
      props: {
        content: string
        readOnly?: boolean
        onDirtyChange?: (d: boolean) => void
      },
      ref: React.Ref<{ getValue: () => string }>,
    ) {
      const valueRef = react.useRef(props.content)
      react.useImperativeHandle(ref, () => ({ getValue: () => valueRef.current }))
      return react.createElement('textarea', {
        'data-testid': 'code-editor',
        defaultValue: props.content,
        readOnly: props.readOnly,
        onChange: (e: { target: { value: string } }) => {
          valueRef.current = e.target.value
          props.onDirtyChange?.(true)
        },
      })
    }),
  }
})

import {
  parseFlowgroupFile,
  selectFlowgroup,
  serializeFlowgroupFile,
} from '@/lib/flowgroup-doc'
import type { DesignerMutator } from '../formModel'
import { CodeModal, type CodeTarget } from '../CodeModal'

const BASE = `pipeline: p
flowgroup: fg
actions:
  - name: a1
    type: transform
    transform_type: sql
    sql: SELECT 1
    target: v_out
`

const fetchMock = vi.fn<(url: string, init?: RequestInit) => Promise<Response>>()
function fileGet(): Response {
  return new Response('SELECT old', { status: 200, headers: { ETag: '"efile"' } })
}
function putOk(): Response {
  return new Response(
    JSON.stringify({ written: true, path: 'x', yaml_error: null, etag: 'e2' }),
    { status: 200 },
  )
}

let queryClient: QueryClient
function renderModal(target: CodeTarget | null, overrides: Partial<Parameters<typeof CodeModal>[0]> = {}) {
  const props = {
    target,
    readOnly: false,
    commit: vi.fn(),
    openAsFile: vi.fn(),
    onClose: vi.fn(),
    ...overrides,
  }
  function Wrapper({ children }: { children: ReactNode }) {
    return <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
  }
  render(<CodeModal {...props} />, { wrapper: Wrapper })
  return props
}

beforeEach(() => {
  vi.clearAllMocks()
  vi.stubGlobal('fetch', fetchMock)
  queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } })
})
afterEach(() => vi.unstubAllGlobals())

const INLINE: CodeTarget = {
  backing: 'inline',
  actionId: 'a1',
  path: ['sql'],
  title: 'SQL',
  language: 'sql',
  initialValue: 'SELECT 1',
}
const FILE: CodeTarget = { backing: 'file', title: 'SQL file', filePath: 'sql/x.sql' }

describe('CodeModal — inline write-back', () => {
  it('Save commits the edited value through setActionField', async () => {
    const captured: DesignerMutator[] = []
    renderModal(INLINE, { commit: (m: DesignerMutator) => captured.push(m) })

    const editor = await screen.findByTestId('code-editor')
    fireEvent.change(editor, { target: { value: 'SELECT 2' } })
    fireEvent.click(screen.getByRole('button', { name: 'Save' }))

    expect(captured).toHaveLength(1)
    // Applying the mutator to a real doc proves it writes the field.
    const file = parseFlowgroupFile(BASE)
    captured[0](selectFlowgroup(file, 'fg')!)
    expect(serializeFlowgroupFile(file)).toContain('SELECT 2')
  })

  it('read-only hides Save (the dirty-buffer guard blocks the write)', async () => {
    const commit = vi.fn()
    renderModal(INLINE, { readOnly: true, commit })
    await screen.findByTestId('code-editor')
    expect(screen.queryByRole('button', { name: 'Save' })).toBeNull()
    expect(commit).not.toHaveBeenCalled()
  })
})

describe('CodeModal — file-backed write-back', () => {
  it('Save PUTs the file with the fetched If-Match etag and body', async () => {
    fetchMock.mockImplementation((_url, init) =>
      Promise.resolve((init?.method ?? 'GET') === 'PUT' ? putOk() : fileGet()),
    )
    renderModal(FILE)

    const editor = await screen.findByTestId('code-editor')
    fireEvent.change(editor, { target: { value: 'SELECT new' } })
    fireEvent.click(screen.getByRole('button', { name: 'Save' }))

    await waitFor(() =>
      expect(fetchMock.mock.calls.some(([, init]) => init?.method === 'PUT')).toBe(true),
    )
    const put = fetchMock.mock.calls.find(([, init]) => init?.method === 'PUT')!
    expect(put[0]).toBe('/api/files/sql/x.sql')
    expect((put[1]!.headers as Record<string, string>)['If-Match']).toBe('"efile"')
    expect(JSON.parse(put[1]!.body as string)).toEqual({ content: 'SELECT new' })
  })

  it('read-only hides Save so no PUT is issued (only the read GET)', async () => {
    fetchMock.mockResolvedValue(fileGet())
    renderModal(FILE, { readOnly: true })
    await screen.findByTestId('code-editor')
    expect(screen.queryByRole('button', { name: 'Save' })).toBeNull()
    expect(fetchMock.mock.calls.every(([, init]) => (init?.method ?? 'GET') === 'GET')).toBe(true)
  })
})

describe('CodeModal — open as file tab', () => {
  it('opens the file as a workspace buffer and closes the modal', async () => {
    fetchMock.mockResolvedValue(fileGet())
    const props = renderModal(FILE)
    fireEvent.click(screen.getByRole('button', { name: /open as file tab/i }))
    expect(props.openAsFile).toHaveBeenCalledWith('sql/x.sql')
    expect(props.onClose).toHaveBeenCalled()
  })
})
