import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import type { ReactNode } from 'react'
import { fireEvent, render, screen, waitFor } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'

vi.mock('sonner', () => ({ toast: { error: vi.fn(), success: vi.fn() } }))

import { CodeFieldRow } from '../ActionFormCodeField'
import type { CodeFieldRowProps } from '../ActionFormCodeField'
import type { CodeTarget } from '../CodeModal'
import type { FieldSpec } from '../specs/types'

const fetchMock = vi.fn<(url: string, init?: RequestInit) => Promise<Response>>()
function notFound(): Response {
  return new Response(JSON.stringify({ detail: 'not found' }), { status: 404 })
}
function found(): Response {
  return new Response('SELECT 1', { status: 200, headers: { ETag: '"e1"' } })
}
function putOk(): Response {
  return new Response(
    JSON.stringify({ written: true, path: 'x', yaml_error: null, etag: 'e2' }),
    { status: 200 },
  )
}

let queryClient: QueryClient
function renderRow(overrides: Partial<CodeFieldRowProps>) {
  const onEditCode = vi.fn<(t: CodeTarget) => void>()
  const props: CodeFieldRowProps = {
    id: 'af-x',
    field: { path: ['sql'], label: 'SQL', widget: 'textarea' } as FieldSpec,
    code: { backing: 'inline', inlineLanguage: 'sql' },
    value: 'SELECT 1',
    actionId: 'a1',
    issue: undefined,
    disabled: false,
    onSet: vi.fn(),
    onUnset: vi.fn(),
    onEditCode,
    ...overrides,
  }
  function Wrapper({ children }: { children: ReactNode }) {
    return <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
  }
  render(<CodeFieldRow {...props} />, { wrapper: Wrapper })
  return { onEditCode }
}

const SQL_PATH_FIELD = { path: ['sql_path'], label: 'SQL file', widget: 'text' } as FieldSpec
const FILE_CODE = { backing: 'file', inlineLanguage: 'sql' } as const

beforeEach(() => {
  vi.clearAllMocks()
  vi.stubGlobal('fetch', fetchMock)
  queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } })
})
afterEach(() => vi.unstubAllGlobals())

describe('inline code field', () => {
  it('Edit in editor opens the modal on the inline value', () => {
    const { onEditCode } = renderRow({})
    fireEvent.click(screen.getByRole('button', { name: /edit in editor/i }))
    expect(onEditCode).toHaveBeenCalledWith({
      backing: 'inline',
      actionId: 'a1',
      path: ['sql'],
      title: 'SQL',
      language: 'sql',
      initialValue: 'SELECT 1',
    })
  })
})

describe('companion file field', () => {
  it('offers Create when the file is missing, then PUTs a stub and opens the modal', async () => {
    fetchMock.mockImplementation((_url, init) =>
      Promise.resolve((init?.method ?? 'GET') === 'PUT' ? putOk() : notFound()),
    )
    const { onEditCode } = renderRow({
      field: SQL_PATH_FIELD,
      code: FILE_CODE,
      value: 'sql/new.sql',
    })

    const createBtn = await screen.findByRole('button', { name: /create file/i })
    fireEvent.click(createBtn)

    await waitFor(() =>
      expect(fetchMock.mock.calls.some(([, init]) => init?.method === 'PUT')).toBe(true),
    )
    const put = fetchMock.mock.calls.find(([, init]) => init?.method === 'PUT')!
    expect(put[0]).toBe('/api/files/sql/new.sql')
    expect(JSON.parse(put[1]!.body as string)).toEqual({ content: '-- SQL\n' })

    await waitFor(() =>
      expect(onEditCode).toHaveBeenCalledWith({
        backing: 'file',
        title: 'SQL file',
        filePath: 'sql/new.sql',
      }),
    )
  })

  it('offers Edit file when the file exists', async () => {
    fetchMock.mockResolvedValue(found())
    const { onEditCode } = renderRow({
      field: SQL_PATH_FIELD,
      code: FILE_CODE,
      value: 'sql/there.sql',
    })
    const editBtn = await screen.findByRole('button', { name: /edit file/i })
    fireEvent.click(editBtn)
    expect(onEditCode).toHaveBeenCalledWith({
      backing: 'file',
      title: 'SQL file',
      filePath: 'sql/there.sql',
    })
    expect(fetchMock.mock.calls.every(([, init]) => (init?.method ?? 'GET') === 'GET')).toBe(true)
  })

  it('withholds Create when read-only (dirty-buffer guard)', async () => {
    fetchMock.mockResolvedValue(notFound())
    renderRow({ field: SQL_PATH_FIELD, code: FILE_CODE, value: 'sql/new.sql', disabled: true })
    await waitFor(() => expect(screen.getByText(/doesn't exist yet/i)).toBeInTheDocument())
    expect(screen.queryByRole('button', { name: /create file/i })).toBeNull()
  })
})
