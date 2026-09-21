import { getSessionId } from '../lib/session-id'
import { getToken } from '../lib/session-token'

// ── Push channel transport (GET /api/events) ─────────────
//
// Server-push SSE endpoint consumed with the browser-native EventSource
// (the endpoint is a plain GET, unlike the POST-with-body run streams in
// `stream.ts`). EventSource cannot set request headers, so what the other
// clients send via `authHeaders()` travels as query parameters instead:
// the session token as `?token=` (omitted entirely when none is present,
// a no-op against a tokenless backend) and the per-tab session id as
// `session=` (always present; see lib/session-id).

const BASE_URL = import.meta.env.VITE_API_BASE_URL ?? '/api'

/** SSE `event:` name for batched file-system changes. */
export const FILE_CHANGED_EVENT = 'file-changed'
/** SSE `event:` name for validate/generate run lifecycle updates. */
export const RUN_UPDATED_EVENT = 'run-updated'
/**
 * SSE `event:` name fired when a graph-relevant edit made the served
 * dependency graph stale. The SPA sets a client stale flag (surfacing a
 * "Refresh" affordance) rather than refetching the graph.
 */
export const GRAPH_STALE_EVENT = 'graph-stale'

export type { FileChangedPayload, RunUpdatedPayload } from '../types/push'

/**
 * Open the push channel. Returns `null` when the environment has no
 * `EventSource` (non-browser context); callers treat that as "no push
 * channel" and rely on manual refetches.
 */
export function createPushSource(): EventSource | null {
  if (typeof EventSource === 'undefined') return null
  const token = getToken()
  const query =
    token !== null
      ? `token=${encodeURIComponent(token)}&session=${getSessionId()}`
      : `session=${getSessionId()}`
  return new EventSource(`${BASE_URL}/events?${query}`)
}
