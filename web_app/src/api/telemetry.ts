import type { UiEventsRequest } from '../types/api'
import { authHeaders } from './client'

const BASE_URL = import.meta.env.VITE_API_BASE_URL ?? '/api'

/**
 * Post one batch of UI-surface events to `POST /api/telemetry/ui`.
 *
 * Fire-and-forget by contract: the route answers 204 with no body, and
 * telemetry must never surface to the user, so the returned promise always
 * resolves — a network failure or a non-2xx status is swallowed here rather
 * than routed through `fetchApi` (which throws and whose callers toast).
 * `keepalive` lets a batch posted from `pagehide` outlive the page. The
 * caller keeps `events` at or under the backend's per-request cap (100).
 */
export function postUiEvents(body: UiEventsRequest): Promise<void> {
  return fetch(`${BASE_URL}/telemetry/ui`, {
    method: 'POST',
    keepalive: true,
    headers: { 'Content-Type': 'application/json', ...authHeaders() },
    body: JSON.stringify(body),
  })
    .then(() => undefined)
    .catch(() => {})
}
