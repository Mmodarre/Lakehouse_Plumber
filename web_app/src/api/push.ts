import { getToken } from '../lib/session-token'

// ── Push channel transport (GET /api/events) ─────────────
//
// Server-push SSE endpoint consumed with the browser-native EventSource
// (the endpoint is a plain GET, unlike the POST-with-body run streams in
// `stream.ts`). EventSource cannot set request headers, so the session
// token travels as a `?token=` query parameter instead of X-LHP-Token;
// when no token is present the parameter is omitted entirely, which is a
// no-op against a tokenless backend.

const BASE_URL = import.meta.env.VITE_API_BASE_URL ?? '/api'

/** SSE `event:` name for batched file-system changes. */
export const FILE_CHANGED_EVENT = 'file-changed'
/** SSE `event:` name for validate/generate run lifecycle updates. */
export const RUN_UPDATED_EVENT = 'run-updated'

export type { FileChangedPayload, RunUpdatedPayload } from '../types/push'

/**
 * Open the push channel. Returns `null` when the environment has no
 * `EventSource` (non-browser context); callers treat that as "no push
 * channel" and rely on manual refetches.
 */
export function createPushSource(): EventSource | null {
  if (typeof EventSource === 'undefined') return null
  const token = getToken()
  const url =
    token !== null
      ? `${BASE_URL}/events?token=${encodeURIComponent(token)}`
      : `${BASE_URL}/events`
  return new EventSource(url)
}
