import type { ErrorDetail } from '../types/api'
import { getSessionId } from '../lib/session-id'
import { getToken } from '../lib/session-token'

const BASE_URL = import.meta.env.VITE_API_BASE_URL ?? '/api'

export class ApiError extends Error {
  status: number
  code: string
  suggestions: string[]

  constructor(status: number, detail: ErrorDetail) {
    super(detail.message)
    this.name = 'ApiError'
    this.status = status
    this.code = detail.code
    this.suggestions = detail.suggestions
  }
}

// Headers that attribute a request to this tab. `X-LHP-Session` (see
// lib/session-id) is always present so the backend can count the call
// against the tab's telemetry session. `X-LHP-Token` is added only when the
// server was launched with a token (stored by lib/session-token), so a
// tokenless (older) backend sees no token header at all. Shared with the run
// and assistant streams (./stream, ./assistant) so every /api call carries
// the same pair.
export function authHeaders(): Record<string, string> {
  const token = getToken()
  return {
    'X-LHP-Session': getSessionId(),
    ...(token ? { 'X-LHP-Token': token } : {}),
  }
}

// Shared non-2xx handler for the JSON/text fetch helpers and the run-stream
// opener (./stream). Always throws.
export async function raiseApiError(response: Response): Promise<never> {
  let detail: ErrorDetail | undefined
  let detailMessage: string | undefined
  try {
    const body = await response.json()
    detail = body.error ?? body
    // FastAPI HTTPException bodies are shaped `{ detail: "..." }` with
    // no structured `error`/`code`; fall back to that string for the
    // message before the generic fallback below.
    if (!body.error && typeof body.detail === 'string') {
      detailMessage = body.detail
    }
  } catch {
    // non-JSON error response
  }

  if (detail?.code) {
    throw new ApiError(response.status, detail)
  }
  throw new ApiError(response.status, {
    code: 'UNKNOWN_ERROR',
    category: 'unknown',
    message:
      detailMessage ??
      `Request failed: ${response.status} ${response.statusText}`,
    details: '',
    suggestions: [],
    context: {},
    http_status: response.status,
  })
}

export async function fetchApi<T>(
  path: string,
  options: RequestInit = {},
): Promise<T> {
  const url = `${BASE_URL}${path}`
  const response = await fetch(url, {
    ...options,
    headers: {
      'Content-Type': 'application/json',
      ...authHeaders(),
      ...options.headers,
    },
  })

  if (!response.ok) await raiseApiError(response)

  return response.json() as Promise<T>
}

// Text-mode sibling of `fetchApi`: returns the raw response body as a string
// instead of parsing JSON, plus the response `ETag` (weak `W/` prefix and
// surrounding quotes stripped, so callers store the raw token). Used for
// `GET /api/files/{path}`, which serves the file's plain-text content (not a
// JSON envelope). Error handling mirrors `fetchApi` — non-2xx responses raise
// `ApiError` from the JSON error body. `etag` is `null` when the backend
// omits the header (older backend).
export async function fetchApiTextWithMeta(
  path: string,
  options: RequestInit = {},
): Promise<{ content: string; etag: string | null }> {
  const url = `${BASE_URL}${path}`
  const response = await fetch(url, {
    ...options,
    headers: {
      'Content-Type': 'application/json',
      ...authHeaders(),
      ...options.headers,
    },
  })

  if (!response.ok) await raiseApiError(response)

  const rawEtag = response.headers.get('ETag')
  const etag = rawEtag
    ? rawEtag.replace(/^W\//, '').replace(/^"(.*)"$/, '$1')
    : null
  const content = await response.text()
  return { content, etag }
}
