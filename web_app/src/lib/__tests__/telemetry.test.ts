import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

// Client contract for the UI-surface telemetry queue: nothing leaves the tab
// until the client has been enabled, an event is a surface name and an
// action (plus `via` for a creation), the queue is bounded, and every post is
// a fire-and-forget batch of at most 100 events carrying the tab's session
// headers.

const TOKEN_KEY = 'lhp-webapp-token'
const SESSION_KEY = 'lhp-web-session'
const SESSION = '0f1e2d3c-4b5a-4978-8765-4321fedcba98'

interface SentEvent {
  surface: string
  action: string
  via?: string
}

const fetchMock = vi.fn<typeof fetch>()

type Client = typeof import('../telemetry')

// Module state (enabled flag, queue, interval, install guard) must start
// clean for every scenario, so each one imports a fresh copy of the client.
// The page-lifecycle listeners a scenario installs stay on the shared jsdom
// window, so each client is disabled on teardown: a stale listener then has
// nothing to post.
let client: Client | null = null
async function freshClient(): Promise<Client> {
  vi.resetModules()
  client = await import('../telemetry')
  return client
}

// The test runner schedules timers of its own around a dynamic import, so
// the client's interval is observed through the global it calls, not by
// counting pending timers.
let setIntervalSpy: ReturnType<typeof vi.spyOn>
let clearIntervalSpy: ReturnType<typeof vi.spyOn>

function sentInit(call = 0): RequestInit {
  return (fetchMock.mock.calls[call]?.[1] ?? {}) as RequestInit
}

function sentBody(call = 0): { session_id: string; events: SentEvent[] } {
  return JSON.parse(String(sentInit(call).body))
}

function setVisibility(state: 'hidden' | 'visible') {
  Object.defineProperty(document, 'visibilityState', { value: state, configurable: true })
  document.dispatchEvent(new Event('visibilitychange'))
}

beforeEach(() => {
  vi.useFakeTimers()
  setIntervalSpy = vi.spyOn(globalThis, 'setInterval')
  clearIntervalSpy = vi.spyOn(globalThis, 'clearInterval')
  fetchMock.mockReset()
  fetchMock.mockResolvedValue(new Response(null, { status: 204 }))
  vi.stubGlobal('fetch', fetchMock)
  sessionStorage.clear()
  sessionStorage.setItem(SESSION_KEY, SESSION)
})

afterEach(() => {
  client?.setTelemetryEnabled(false)
  client = null
  vi.restoreAllMocks()
  vi.unstubAllGlobals()
  vi.useRealTimers()
  sessionStorage.clear()
})

describe('before telemetry is enabled', () => {
  it('does nothing at import: no listeners, no timers, no request', async () => {
    const docSpy = vi.spyOn(document, 'addEventListener')
    const winSpy = vi.spyOn(window, 'addEventListener')
    await freshClient()
    expect(docSpy).not.toHaveBeenCalled()
    expect(winSpy).not.toHaveBeenCalled()
    expect(setIntervalSpy).not.toHaveBeenCalled()
    expect(fetchMock).not.toHaveBeenCalled()
  })

  it('drops tracked events and never posts', async () => {
    const t = await freshClient()
    t.track('file_editor', 'opened')
    t.track('sandbox_control', 'toggled')
    t.flush()
    await vi.advanceTimersByTimeAsync(60_000)
    expect(fetchMock).not.toHaveBeenCalled()

    // Enabling afterwards must not resurrect what was dropped.
    t.setTelemetryEnabled(true)
    t.flush()
    expect(fetchMock).not.toHaveBeenCalled()
  })
})

describe('once enabled', () => {
  it('posts the queue every 15 s with keepalive, the session headers and a {session_id, events} body', async () => {
    sessionStorage.setItem(TOKEN_KEY, 'tok-1')
    const t = await freshClient()
    t.setTelemetryEnabled(true)
    t.track('project_map', 'opened')
    t.track('sandbox_control', 'toggled')

    await vi.advanceTimersByTimeAsync(14_999)
    expect(fetchMock).not.toHaveBeenCalled()
    await vi.advanceTimersByTimeAsync(1)
    expect(fetchMock).toHaveBeenCalledTimes(1)

    expect(fetchMock.mock.calls[0]?.[0]).toBe('/api/telemetry/ui')
    const init = sentInit()
    expect(init.method).toBe('POST')
    expect(init.keepalive).toBe(true)
    expect(init.headers).toEqual({
      'Content-Type': 'application/json',
      'X-LHP-Session': SESSION,
      'X-LHP-Token': 'tok-1',
    })
    expect(sentBody()).toEqual({
      session_id: SESSION,
      events: [
        { surface: 'project_map', action: 'opened' },
        { surface: 'sandbox_control', action: 'toggled' },
      ],
    })
  })

  it('omits the token header when no token is stored', async () => {
    const t = await freshClient()
    t.setTelemetryEnabled(true)
    t.track('project_map', 'opened')
    t.flush()
    expect(sentInit().headers).toEqual({
      'Content-Type': 'application/json',
      'X-LHP-Session': SESSION,
    })
  })

  it('posts nothing on an idle tick', async () => {
    const t = await freshClient()
    t.setTelemetryEnabled(true)
    await vi.advanceTimersByTimeAsync(45_000)
    expect(fetchMock).not.toHaveBeenCalled()
  })

  it('arms a single interval however often it is enabled', async () => {
    const t = await freshClient()
    t.setTelemetryEnabled(true)
    t.setTelemetryEnabled(true)
    t.setTelemetryEnabled(true)
    expect(setIntervalSpy).toHaveBeenCalledTimes(1)
    t.track('problems', 'opened')
    await vi.advanceTimersByTimeAsync(15_000)
    expect(fetchMock).toHaveBeenCalledTimes(1)
  })

  it('collapses consecutive identical opened events but never toggled ones', async () => {
    const t = await freshClient()
    t.setTelemetryEnabled(true)
    t.track('file_editor', 'opened')
    t.track('file_editor', 'opened')
    t.track('file_editor', 'opened')
    t.track('viewer_mode', 'toggled')
    t.track('viewer_mode', 'toggled')
    t.track('file_editor', 'opened')
    t.track('flowgroup_graph', 'opened')
    t.track('file_editor', 'opened')
    t.flush()
    expect(sentBody().events).toEqual([
      { surface: 'file_editor', action: 'opened' },
      { surface: 'viewer_mode', action: 'toggled' },
      { surface: 'viewer_mode', action: 'toggled' },
      { surface: 'file_editor', action: 'opened' },
      { surface: 'flowgroup_graph', action: 'opened' },
      { surface: 'file_editor', action: 'opened' },
    ])
  })

  it('carries via only on a created event', async () => {
    const t = await freshClient()
    t.setTelemetryEnabled(true)
    t.track('create_flowgroup_dialog', 'created', 'template')
    t.track('create_flowgroup_dialog', 'created', 'blueprint')
    t.track('create_flowgroup_dialog', 'created')
    t.track('create_flowgroup_dialog', 'opened', 'blank')
    t.flush()
    const events = sentBody().events
    expect(events).toEqual([
      { surface: 'create_flowgroup_dialog', action: 'created', via: 'template' },
      { surface: 'create_flowgroup_dialog', action: 'created', via: 'blueprint' },
      { surface: 'create_flowgroup_dialog', action: 'created' },
      { surface: 'create_flowgroup_dialog', action: 'opened' },
    ])
    expect(events[2]).not.toHaveProperty('via')
    expect(events[3]).not.toHaveProperty('via')
  })

  it('splits a flush into requests of at most 100 events', async () => {
    const t = await freshClient()
    t.setTelemetryEnabled(true)
    for (let i = 0; i < 150; i += 1) t.track('viewer_mode', 'toggled')
    t.flush()
    expect(fetchMock).toHaveBeenCalledTimes(2)
    expect(sentBody(0).events).toHaveLength(100)
    expect(sentBody(1).events).toHaveLength(50)
    expect(sentBody(0).session_id).toBe(SESSION)
    expect(sentBody(1).session_id).toBe(SESSION)

    // The boundary: exactly 100 is one request, 101 is two.
    fetchMock.mockClear()
    for (let i = 0; i < 100; i += 1) t.track('viewer_mode', 'toggled')
    t.flush()
    expect(fetchMock).toHaveBeenCalledTimes(1)
    fetchMock.mockClear()
    for (let i = 0; i < 101; i += 1) t.track('viewer_mode', 'toggled')
    t.flush()
    expect(fetchMock).toHaveBeenCalledTimes(2)
    expect(sentBody(1).events).toHaveLength(1)
  })

  it('keeps only the newest 200 events when the queue overflows', async () => {
    const t = await freshClient()
    t.setTelemetryEnabled(true)
    for (let i = 0; i < 50; i += 1) t.track('sandbox_control', 'toggled')
    for (let i = 0; i < 200; i += 1) t.track('viewer_mode', 'toggled')
    t.flush()
    expect(fetchMock).toHaveBeenCalledTimes(2)
    const events = [...sentBody(0).events, ...sentBody(1).events]
    expect(events).toHaveLength(200)
    expect(events.every((e) => e.surface === 'viewer_mode')).toBe(true)
  })

  it('flushes on pagehide and when the document becomes hidden, once installed', async () => {
    const t = await freshClient()
    t.setTelemetryEnabled(true)
    t.installTelemetry()

    t.track('run_stream', 'opened')
    window.dispatchEvent(new Event('pagehide'))
    expect(fetchMock).toHaveBeenCalledTimes(1)
    expect(sentBody(0).events).toEqual([{ surface: 'run_stream', action: 'opened' }])

    t.track('run_history', 'opened')
    setVisibility('hidden')
    expect(fetchMock).toHaveBeenCalledTimes(2)
    expect(sentBody(1).events).toEqual([{ surface: 'run_history', action: 'opened' }])

    t.track('problems', 'opened')
    setVisibility('visible')
    expect(fetchMock).toHaveBeenCalledTimes(2)
  })

  it('installs the page-lifecycle listeners once however often installTelemetry runs', async () => {
    const t = await freshClient()
    t.setTelemetryEnabled(true)
    t.installTelemetry()
    t.installTelemetry()
    t.installTelemetry()
    t.track('assistant_panel', 'opened')
    window.dispatchEvent(new Event('pagehide'))
    expect(fetchMock).toHaveBeenCalledTimes(1)
  })

  it('swallows a rejected post and a non-2xx response', async () => {
    const t = await freshClient()
    t.setTelemetryEnabled(true)

    fetchMock.mockRejectedValueOnce(new TypeError('network down'))
    t.track('problems', 'opened')
    expect(() => t.flush()).not.toThrow()
    await vi.advanceTimersByTimeAsync(0)

    fetchMock.mockResolvedValueOnce(new Response('{"detail":"nope"}', { status: 422 }))
    t.track('problems', 'opened')
    expect(() => t.flush()).not.toThrow()
    await vi.advanceTimersByTimeAsync(0)

    expect(fetchMock).toHaveBeenCalledTimes(2)
  })

  it('disabling clears the queue and stops the interval', async () => {
    const t = await freshClient()
    t.setTelemetryEnabled(true)
    t.track('project_map', 'opened')
    t.track('pipeline_dag', 'opened')
    t.setTelemetryEnabled(false)
    expect(clearIntervalSpy).toHaveBeenCalledTimes(1)
    t.flush()
    await vi.advanceTimersByTimeAsync(30_000)
    expect(fetchMock).not.toHaveBeenCalled()

    // Re-enabling starts from an empty queue; only new events are posted.
    t.setTelemetryEnabled(true)
    await vi.advanceTimersByTimeAsync(15_000)
    expect(fetchMock).not.toHaveBeenCalled()
    t.track('table_detail', 'opened')
    await vi.advanceTimersByTimeAsync(15_000)
    expect(fetchMock).toHaveBeenCalledTimes(1)
    expect(sentBody().events).toEqual([{ surface: 'table_detail', action: 'opened' }])
  })

  it('never writes to the console', async () => {
    const spies = (['log', 'debug', 'info', 'warn', 'error'] as const).map((level) =>
      vi.spyOn(console, level).mockImplementation(() => {}),
    )
    fetchMock.mockRejectedValue(new TypeError('network down'))
    const t = await freshClient()
    t.setTelemetryEnabled(true)
    t.installTelemetry()
    t.track('files_lens', 'opened')
    t.flush()
    window.dispatchEvent(new Event('pagehide'))
    await vi.advanceTimersByTimeAsync(15_000)
    t.setTelemetryEnabled(false)
    for (const spy of spies) expect(spy).not.toHaveBeenCalled()
  })
})
