import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

// The shim is the only telemetry entry point the eager app chunk may import.
// It must cost nothing at import, hold what components report before the
// lazily-loaded client has been enabled, and hand everything to the client
// the moment it is.

const SESSION_KEY = 'lhp-web-session'
const SESSION = '0f1e2d3c-4b5a-4978-8765-4321fedcba98'

const fetchMock = vi.fn<typeof fetch>()

async function fresh() {
  vi.resetModules()
  const shim = await import('../telemetry-shim')
  const client = await import('../telemetry')
  return { shim, client }
}

function sentEvents(call = 0): { surface: string; action: string; via?: string }[] {
  const init = fetchMock.mock.calls[call]?.[1]
  return JSON.parse(String(init?.body)).events
}

beforeEach(() => {
  vi.useFakeTimers()
  fetchMock.mockReset()
  fetchMock.mockResolvedValue(new Response(null, { status: 204 }))
  vi.stubGlobal('fetch', fetchMock)
  sessionStorage.clear()
  sessionStorage.setItem(SESSION_KEY, SESSION)
})

afterEach(() => {
  vi.unstubAllGlobals()
  vi.useRealTimers()
  sessionStorage.clear()
})

describe('telemetry shim', () => {
  it('accepts events before the client exists without posting anything', async () => {
    vi.resetModules()
    const shim = await import('../telemetry-shim')
    expect(() => shim.track('init_wizard', 'opened')).not.toThrow()
    await vi.advanceTimersByTimeAsync(60_000)
    expect(fetchMock).not.toHaveBeenCalled()
  })

  it('replays what it held once the client is enabled, then forwards live', async () => {
    const { shim, client } = await fresh()
    shim.track('init_wizard', 'opened')
    shim.track('sandbox_control', 'toggled')

    client.setTelemetryEnabled(true)
    shim.track('create_flowgroup_dialog', 'created', 'blank')
    client.flush()

    expect(fetchMock).toHaveBeenCalledTimes(1)
    expect(sentEvents()).toEqual([
      { surface: 'init_wizard', action: 'opened' },
      { surface: 'sandbox_control', action: 'toggled' },
      { surface: 'create_flowgroup_dialog', action: 'created', via: 'blank' },
    ])
  })

  it('replays each held event once', async () => {
    const { shim, client } = await fresh()
    shim.track('init_wizard', 'opened')
    client.setTelemetryEnabled(true)
    client.flush()
    client.setTelemetryEnabled(false)
    client.setTelemetryEnabled(true)
    client.flush()
    expect(fetchMock).toHaveBeenCalledTimes(1)
  })

  it('bounds what it holds while the client is absent', async () => {
    const { shim, client } = await fresh()
    for (let i = 0; i < 500; i += 1) shim.track('viewer_mode', 'toggled')
    client.setTelemetryEnabled(true)
    client.flush()
    expect(fetchMock).toHaveBeenCalledTimes(1)
    expect(sentEvents().length).toBeLessThanOrEqual(shim.PENDING_CAP)
    expect(sentEvents().length).toBeGreaterThan(0)
  })

  it('drops events at the client while it is disabled', async () => {
    const { shim, client } = await fresh()
    client.setTelemetryEnabled(true)
    client.setTelemetryEnabled(false)
    shim.track('problems', 'opened')
    client.setTelemetryEnabled(true)
    client.flush()
    expect(fetchMock).not.toHaveBeenCalled()
  })
})
