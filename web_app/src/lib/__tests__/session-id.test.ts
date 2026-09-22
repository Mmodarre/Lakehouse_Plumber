import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

// Per-tab session id contract: minted once, persisted in sessionStorage under
// `lhp-web-session`, stable for the tab's lifetime even when storage is
// unavailable, and always a lowercase v4 uuid — the only spelling the backend
// accepts (anything else is treated as "no session").

const SESSION_KEY = 'lhp-web-session'
const UUID_V4 =
  /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/

// The module caches its in-memory fallback, so each test starts from a fresh
// module instance to exercise the mint path deterministically.
async function freshModule() {
  vi.resetModules()
  return import('../session-id')
}

beforeEach(() => {
  sessionStorage.clear()
})

afterEach(() => {
  vi.restoreAllMocks()
  vi.unstubAllGlobals()
  sessionStorage.clear()
})

describe('getSessionId', () => {
  it('mints a lowercase v4 uuid once and returns the same id on every call', async () => {
    const { getSessionId } = await freshModule()
    const first = getSessionId()
    expect(first).toMatch(UUID_V4)
    expect(getSessionId()).toBe(first)
    expect(getSessionId()).toBe(first)
    expect(sessionStorage.getItem(SESSION_KEY)).toBe(first)
  })

  it('reuses a stored id after a module reload (a page reload keeps the tab session)', async () => {
    sessionStorage.setItem(SESSION_KEY, '0f1e2d3c-4b5a-4978-8765-4321fedcba98')
    const { getSessionId } = await freshModule()
    expect(getSessionId()).toBe('0f1e2d3c-4b5a-4978-8765-4321fedcba98')
  })

  it('replaces a stored value that is not a lowercase v4 uuid', async () => {
    sessionStorage.setItem(SESSION_KEY, '0F1E2D3C-4B5A-4978-8765-4321FEDCBA98')
    const { getSessionId } = await freshModule()
    const id = getSessionId()
    expect(id).toMatch(UUID_V4)
    expect(id).not.toBe('0F1E2D3C-4B5A-4978-8765-4321FEDCBA98')
    expect(sessionStorage.getItem(SESSION_KEY)).toBe(id)
  })

  it('stays stable when sessionStorage throws (private mode / disabled storage)', async () => {
    vi.spyOn(Storage.prototype, 'getItem').mockImplementation(() => {
      throw new Error('storage blocked')
    })
    vi.spyOn(Storage.prototype, 'setItem').mockImplementation(() => {
      throw new Error('storage blocked')
    })
    const { getSessionId } = await freshModule()
    const first = getSessionId()
    expect(first).toMatch(UUID_V4)
    expect(getSessionId()).toBe(first)
  })

  it('uses crypto.randomUUID when the runtime provides it', async () => {
    const randomUUID = vi.fn(() => 'aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee')
    vi.stubGlobal('crypto', { randomUUID })
    const { getSessionId } = await freshModule()
    expect(getSessionId()).toBe('aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee')
    expect(getSessionId()).toBe('aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee')
    expect(randomUUID).toHaveBeenCalledTimes(1)
  })

  it('falls back to a Math.random v4 uuid when crypto.randomUUID is missing', async () => {
    vi.stubGlobal('crypto', {})
    // With every random nibble pinned to 0 only the fixed version (4) and
    // variant (8) nibbles remain, proving the v4 layout of the fallback.
    vi.spyOn(Math, 'random').mockReturnValue(0)
    const { getSessionId } = await freshModule()
    expect(getSessionId()).toBe('00000000-0000-4000-8000-000000000000')
  })

  it('never writes the id to the console', async () => {
    const spies = (['log', 'debug', 'info', 'warn', 'error'] as const).map((level) =>
      vi.spyOn(console, level).mockImplementation(() => {}),
    )
    const { getSessionId } = await freshModule()
    getSessionId()
    for (const spy of spies) expect(spy).not.toHaveBeenCalled()
  })
})
