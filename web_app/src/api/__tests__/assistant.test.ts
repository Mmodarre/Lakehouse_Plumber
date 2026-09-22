import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { startAssistantChat } from '../assistant'
import { getSessionId } from '../../lib/session-id'
import type { ChatRequest } from '../../types/assistant'

// Request-header contract of the assistant chat stream: it shares
// `authHeaders()` with `fetchApi`, so it carries the per-tab session id on
// every open and the session token exactly when one is stored.

const TOKEN_KEY = 'lhp-webapp-token'

const fetchMock = vi.fn<typeof fetch>()

const body: ChatRequest = { message: 'hello', permission_mode: 'default' }

beforeEach(() => {
  vi.clearAllMocks()
  vi.stubGlobal('fetch', fetchMock)
  sessionStorage.removeItem(TOKEN_KEY)
})

afterEach(() => {
  vi.unstubAllGlobals()
  sessionStorage.removeItem(TOKEN_KEY)
})

function sentHeaders(): Record<string, string> {
  const init = fetchMock.mock.calls[0]?.[1]
  return (init?.headers ?? {}) as Record<string, string>
}

describe('startAssistantChat — request headers', () => {
  it('POSTs NDJSON with X-LHP-Session and no token header when no token is stored', async () => {
    fetchMock.mockResolvedValue(new Response('', { status: 200 }))
    await startAssistantChat(body)
    expect(fetchMock.mock.calls[0]?.[0]).toBe('/api/assistant/chat')
    expect(fetchMock.mock.calls[0]?.[1]?.method).toBe('POST')
    expect(sentHeaders()).toEqual({
      'Content-Type': 'application/json',
      Accept: 'application/x-ndjson',
      'X-LHP-Session': getSessionId(),
    })
  })

  it('adds X-LHP-Token alongside the session header when a token is stored', async () => {
    sessionStorage.setItem(TOKEN_KEY, 'tok-123')
    fetchMock.mockResolvedValue(new Response('', { status: 200 }))
    await startAssistantChat(body)
    expect(sentHeaders()).toEqual({
      'Content-Type': 'application/json',
      Accept: 'application/x-ndjson',
      'X-LHP-Session': getSessionId(),
      'X-LHP-Token': 'tok-123',
    })
  })
})
