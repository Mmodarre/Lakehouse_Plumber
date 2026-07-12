import { raiseApiError } from './client'
import { getToken } from '../lib/session-token'

const BASE_URL = import.meta.env.VITE_API_BASE_URL ?? '/api'

export type StreamPath = '/api/validate/stream' | '/api/generate/stream'

export interface StreamBody {
  env: string
  pipeline?: string
  /**
   * Project-relative pipeline_config path the run should use (the IDE
   * counterpart of the CLI's `--pipeline-config`). Omitted (undefined →
   * dropped by JSON.stringify) when no run config is selected, keeping the
   * wire body identical to the pre-selection shape. The backend guards the
   * path (403 escaping the project root, 404 missing file).
   */
  pipeline_config?: string
  /**
   * Developer-sandbox mode (the CLI's `--sandbox`): scope and namespace come
   * from `.lhp/profile.yaml`. Mutually exclusive with `pipeline` (the backend
   * 422s if both are set), so the controller drops `pipeline` when this is on.
   * Omitted (undefined) for a normal run, keeping the wire body unchanged.
   */
  sandbox?: boolean
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

  // Shared with fetchApi so plain FastAPI `{detail: "..."}` bodies (the
  // pipeline_config 403/404 guards) surface their message instead of a
  // generic "Stream request failed".
  if (!response.ok) await raiseApiError(response)

  return response
}
