import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { createPushSource } from '../push'
import { getSessionId } from '../../lib/session-id'

// URL contract of the push channel: EventSource cannot set headers, so both
// the session token and the per-tab session id travel as query parameters.
// The token keeps its encoding and position; the session id is appended.

const TOKEN_KEY = 'lhp-webapp-token'

class MockEventSource {
  url: string
  constructor(url: string) {
    this.url = url
  }
}

beforeEach(() => {
  vi.stubGlobal('EventSource', MockEventSource)
  sessionStorage.removeItem(TOKEN_KEY)
})

afterEach(() => {
  vi.unstubAllGlobals()
  sessionStorage.removeItem(TOKEN_KEY)
})

function openedUrl(): string {
  const source = createPushSource()
  if (source === null) throw new Error('expected an EventSource')
  return source.url
}

describe('createPushSource', () => {
  it('returns null when the runtime has no EventSource', () => {
    vi.stubGlobal('EventSource', undefined)
    expect(createPushSource()).toBeNull()
  })

  it('carries session=<id> and no token parameter when no token is stored', () => {
    expect(openedUrl()).toBe(`/api/events?session=${getSessionId()}`)
  })

  it('keeps the encoded token parameter first and appends the session id', () => {
    sessionStorage.setItem(TOKEN_KEY, 'tok/with+chars')
    expect(openedUrl()).toBe(
      `/api/events?token=tok%2Fwith%2Bchars&session=${getSessionId()}`,
    )
  })

  it('sends the same session id the API headers use', () => {
    const params = new URL(openedUrl(), 'http://localhost').searchParams
    expect(params.get('session')).toBe(getSessionId())
  })
})
