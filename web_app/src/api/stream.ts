import type { ErrorDetail } from '../types/api'
import { ApiError } from './client'
import { getToken } from '../lib/session-token'

const BASE_URL = import.meta.env.VITE_API_BASE_URL ?? '/api'

export type StreamPath = '/api/validate/stream' | '/api/generate/stream'

export interface StreamBody {
  env: string
  pipeline?: string
}

/**
 * Open an NDJSON event stream over POST.
 *
 * Returns the raw {@link Response} so the consuming hook can read
 * `response.body` as a `ReadableStream` and decode newline-delimited
 * JSON frames (see `StreamFrame` in ../types/api). This is NOT an
 * EventSource — `POST` with a request body is required.
 *
 * Non-2xx responses throw {@link ApiError}, consistent with
 * `fetchApi` in ./client.
 */
export async function startStream(
  path: StreamPath,
  body: StreamBody,
  signal?: AbortSignal,
): Promise<Response> {
  const url = `${BASE_URL}${path.replace(/^\/api/, '')}`
  const token = getToken()
  const response = await fetch(url, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      Accept: 'application/x-ndjson',
      ...(token ? { 'X-LHP-Token': token } : {}),
    },
    body: JSON.stringify(body),
    signal,
  })

  if (!response.ok) {
    let detail: ErrorDetail | undefined
    try {
      const errorBody = await response.json()
      detail = errorBody.error ?? errorBody
    } catch {
      // non-JSON error response
    }

    if (detail?.code) {
      throw new ApiError(response.status, detail)
    }
    throw new ApiError(response.status, {
      code: 'UNKNOWN_ERROR',
      category: 'unknown',
      message: `Stream request failed: ${response.status} ${response.statusText}`,
      details: '',
      suggestions: [],
      context: {},
      http_status: response.status,
    })
  }

  return response
}
