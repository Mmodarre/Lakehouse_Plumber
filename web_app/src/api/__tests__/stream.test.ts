import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { startStream } from '../stream'
import { getSessionId } from '../../lib/session-id'

// Wire-body contract for the run streams: `pipeline_config` must reach the
// backend exactly when a run config is selected, and the serialized body
// must be byte-identical to the pre-feature shape when it is not (undefined
// optionals are dropped by JSON.stringify — asserted, not assumed).

const fetchMock = vi.fn<typeof fetch>()

beforeEach(() => {
  vi.clearAllMocks()
  vi.stubGlobal('fetch', fetchMock)
})

afterEach(() => {
  vi.unstubAllGlobals()
})

function sentBody(): Record<string, unknown> {
  const init = fetchMock.mock.calls[0]?.[1]
  return JSON.parse(init?.body as string) as Record<string, unknown>
}

describe('startStream — wire body', () => {
  it('omits pipeline_config (and pipeline) entirely when not provided — validate', async () => {
    fetchMock.mockResolvedValue(new Response('', { status: 200 }))
    await startStream('/api/validate/stream', {
      env: 'dev',
      pipeline: undefined,
      pipeline_config: undefined,
    })
    expect(sentBody()).toEqual({ env: 'dev' })
    expect(Object.keys(sentBody())).toEqual(['env'])
  })

  it('includes pipeline_config when provided — generate', async () => {
    fetchMock.mockResolvedValue(new Response('', { status: 200 }))
    await startStream('/api/generate/stream', {
      env: 'tst',
      pipeline_config: 'config/pipeline_config_dev.yaml',
    })
    expect(sentBody()).toEqual({
      env: 'tst',
      pipeline_config: 'config/pipeline_config_dev.yaml',
    })
  })

  it('sends `trigger` only when the run is tagged auto', async () => {
    fetchMock.mockResolvedValue(new Response('', { status: 200 }))
    await startStream('/api/validate/stream', { env: 'dev', trigger: 'auto' })
    expect(sentBody()).toEqual({ env: 'dev', trigger: 'auto' })
  })

  it("surfaces FastAPI's plain `detail` string when the open fails (404 guard)", async () => {
    // The backend's pipeline_config guards raise HTTPException, whose body
    // is `{detail: "..."}` with no structured error/code — the message must
    // still reach the user, not collapse to "Stream request failed: 404".
    fetchMock.mockResolvedValue(
      new Response(JSON.stringify({ detail: 'File not found: config/gone.yaml' }), {
        status: 404,
        statusText: 'Not Found',
        headers: { 'Content-Type': 'application/json' },
      }),
    )
    await expect(startStream('/api/validate/stream', { env: 'dev' })).rejects.toMatchObject({
      name: 'ApiError',
      status: 404,
      message: 'File not found: config/gone.yaml',
    })
  })
})

// Request-header contract: the run streams share `authHeaders()` with
// `fetchApi`, so they carry the per-tab session id on every open and the
// session token exactly when one is stored.
describe('startStream — request headers', () => {
  const TOKEN_KEY = 'lhp-webapp-token'

  afterEach(() => {
    sessionStorage.removeItem(TOKEN_KEY)
  })

  function sentHeaders(): Record<string, string> {
    const init = fetchMock.mock.calls[0]?.[1]
    return (init?.headers ?? {}) as Record<string, string>
  }

  it('sends X-LHP-Session and no token header when no token is stored', async () => {
    fetchMock.mockResolvedValue(new Response('', { status: 200 }))
    await startStream('/api/validate/stream', { env: 'dev' })
    expect(sentHeaders()).toEqual({
      'Content-Type': 'application/json',
      Accept: 'application/x-ndjson',
      'X-LHP-Session': getSessionId(),
    })
  })

  it('adds X-LHP-Token alongside the session header when a token is stored', async () => {
    sessionStorage.setItem(TOKEN_KEY, 'tok-123')
    fetchMock.mockResolvedValue(new Response('', { status: 200 }))
    await startStream('/api/generate/stream', { env: 'dev' })
    expect(sentHeaders()).toEqual({
      'Content-Type': 'application/json',
      Accept: 'application/x-ndjson',
      'X-LHP-Session': getSessionId(),
      'X-LHP-Token': 'tok-123',
    })
  })
})
