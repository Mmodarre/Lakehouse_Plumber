import type { ErrorDetail } from '../types/api'

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

export async function fetchApi<T>(
  path: string,
  options: RequestInit = {},
): Promise<T> {
  const url = `${BASE_URL}${path}`
  const response = await fetch(url, {
    ...options,
    headers: {
      'Content-Type': 'application/json',
      ...options.headers,
    },
  })

  if (!response.ok) {
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

  return response.json() as Promise<T>
}

// Text-mode sibling of `fetchApi`: returns the raw response body as a string
// instead of parsing JSON. Used for `GET /api/files/{path}`, which serves the
// file's plain-text content (not a JSON envelope). Error handling mirrors
// `fetchApi` — non-2xx responses raise `ApiError` from the JSON error body.
export async function fetchApiText(
  path: string,
  options: RequestInit = {},
): Promise<string> {
  const url = `${BASE_URL}${path}`
  const response = await fetch(url, {
    ...options,
    headers: {
      'Content-Type': 'application/json',
      ...options.headers,
    },
  })

  if (!response.ok) {
    let detail: ErrorDetail | undefined
    let detailMessage: string | undefined
    try {
      const body = await response.json()
      detail = body.error ?? body
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

  return response.text()
}
