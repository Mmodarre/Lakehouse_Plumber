import { describe, expect, it, vi, beforeEach } from 'vitest'
import type { ReactNode } from 'react'
import { renderHook, waitFor } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'

// useSchemaHelp fetches the schema through loadSchemaCached; stub it so no real
// network GET happens (mirrors api/__tests__/schemas.test.ts mocking style).
vi.mock('../../../api/schemas', () => ({
  loadSchemaCached: vi.fn(),
}))

import { loadSchemaCached } from '../../../api/schemas'
import { SchemaKindProvider, useFieldHelp } from '../SchemaKindContext'

// HELP_ROOT['flowgroup'] === '#/definitions/Action', so field paths resolve
// relative to the Action node.
const stubDoc: Record<string, unknown> = {
  definitions: {
    Action: {
      type: 'object',
      properties: {
        name: { type: 'string', description: 'The action name.' },
      },
    },
  },
}

let queryClient: QueryClient

beforeEach(() => {
  vi.clearAllMocks()
  queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } })
})

function withProvider({ children }: { children: ReactNode }) {
  return (
    <QueryClientProvider client={queryClient}>
      <SchemaKindProvider kind="flowgroup">{children}</SchemaKindProvider>
    </QueryClientProvider>
  )
}

function queryOnly({ children }: { children: ReactNode }) {
  return <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
}

describe('useFieldHelp / SchemaKindProvider', () => {
  it('resolves a field description from the schema once loaded', async () => {
    vi.mocked(loadSchemaCached).mockResolvedValue(stubDoc)
    const { result } = renderHook(() => useFieldHelp(['name']), { wrapper: withProvider })
    await waitFor(() => expect(result.current).toBe('The action name.'))
  })

  it('returns the explicit override regardless of the resolver', async () => {
    vi.mocked(loadSchemaCached).mockResolvedValue(stubDoc)
    const { result } = renderHook(() => useFieldHelp(['name'], 'EXPLICIT'), {
      wrapper: withProvider,
    })
    // Override wins synchronously and stays winning after the schema loads.
    expect(result.current).toBe('EXPLICIT')
    await waitFor(() => expect(loadSchemaCached).toHaveBeenCalled())
    expect(result.current).toBe('EXPLICIT')
  })

  it('returns undefined with no path and no override', () => {
    vi.mocked(loadSchemaCached).mockResolvedValue(stubDoc)
    const { result } = renderHook(() => useFieldHelp(), { wrapper: withProvider })
    expect(result.current).toBeUndefined()
  })

  it('returns undefined before the query resolves (no-op resolver, no throw)', () => {
    vi.mocked(loadSchemaCached).mockReturnValue(new Promise<Record<string, unknown>>(() => {}))
    const { result } = renderHook(() => useFieldHelp(['name']), { wrapper: withProvider })
    expect(result.current).toBeUndefined()
  })

  it('returns undefined outside any SchemaKindProvider', () => {
    const { result } = renderHook(() => useFieldHelp(['name']), { wrapper: queryOnly })
    expect(result.current).toBeUndefined()
  })
})
