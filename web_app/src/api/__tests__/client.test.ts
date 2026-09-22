import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { authHeaders, fetchApi, fetchApiTextWithMeta } from '../client'
import { getSessionId } from '../../lib/session-id'

// Request-header contract of the shared client: every call carries the
// per-tab session id (X-LHP-Session), and the session token (X-LHP-Token)
// exactly when one is stored — never an empty or placeholder token header.

const TOKEN_KEY = 'lhp-webapp-token'

const fetchMock = vi.fn<typeof fetch>()

beforeEach(() => {
  vi.clearAllMocks()
  vi.stubGlobal('fetch', fetchMock)
  sessionStorage.removeItem(TOKEN_KEY)
})

afterEach(() => {
  vi.unstubAllGlobals()
  sessionStorage.removeItem(TOKEN_KEY)
})

function sentHeaders(call = 0): Record<string, string> {
  const init = fetchMock.mock.calls[call]?.[1]
  return (init?.headers ?? {}) as Record<string, string>
}

describe('authHeaders', () => {
  it('carries only the session id when no token is stored', () => {
    expect(authHeaders()).toEqual({ 'X-LHP-Session': getSessionId() })
  })

  it('adds X-LHP-Token alongside the session id when a token is stored', () => {
    sessionStorage.setItem(TOKEN_KEY, 'tok-123')
    expect(authHeaders()).toEqual({
      'X-LHP-Session': getSessionId(),
      'X-LHP-Token': 'tok-123',
    })
  })
})

describe('fetchApi', () => {
  it('sends X-LHP-Session on every request', async () => {
    // A fresh Response per call: a body can only be consumed once.
    fetchMock.mockImplementation(async () => new Response('{}', { status: 200 }))
    await fetchApi('/health')
    await fetchApi('/files', { method: 'POST', body: '{}' })
    expect(fetchMock).toHaveBeenCalledTimes(2)
    expect(sentHeaders(0)['X-LHP-Session']).toBe(getSessionId())
    expect(sentHeaders(1)['X-LHP-Session']).toBe(getSessionId())
    expect(sentHeaders(0)).not.toHaveProperty('X-LHP-Token')
  })

  it('merges caller headers with the session and token headers', async () => {
    sessionStorage.setItem(TOKEN_KEY, 'tok-123')
    fetchMock.mockResolvedValue(new Response('{}', { status: 200 }))
    await fetchApi('/files/a.yaml', { headers: { 'If-Match': 'etag-1' } })
    expect(sentHeaders()).toEqual({
      'Content-Type': 'application/json',
      'X-LHP-Session': getSessionId(),
      'X-LHP-Token': 'tok-123',
      'If-Match': 'etag-1',
    })
  })
})

describe('fetchApiTextWithMeta', () => {
  it('sends X-LHP-Session and still returns the body and ETag', async () => {
    fetchMock.mockResolvedValue(
      new Response('raw: yaml', { status: 200, headers: { ETag: 'W/"abc"' } }),
    )
    const result = await fetchApiTextWithMeta('/files/a.yaml')
    expect(sentHeaders()['X-LHP-Session']).toBe(getSessionId())
    expect(result).toEqual({ content: 'raw: yaml', etag: 'abc' })
  })
})
