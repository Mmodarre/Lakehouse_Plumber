import { useEffect } from 'react'
import { useQueryClient } from '@tanstack/react-query'
import {
  createPushSource,
  FILE_CHANGED_EVENT,
  GRAPH_STALE_EVENT,
  RUN_UPDATED_EVENT,
} from '../api/push'
import type { FileChangedPayload, RunUpdatedPayload } from '../api/push'
import { useGraphStalenessStore } from '../store/graphStalenessStore'

// ── usePushChannel — server-push → query invalidation ────────
//
// Subscribes once (mounted in Layout) to `GET /api/events` and turns the
// two push events into TanStack Query invalidations, so open views
// refetch when the project changes on disk or a run finishes elsewhere.
//
// The server keeps no Last-Event-ID replay buffer, so any reconnect that
// follows a disconnect invalidates ALL queries — cheap, and guaranteed
// not to miss events that fired while the channel was down.
//
// EventSource retries network drops on its own; HTTP error responses
// (404 from an older backend without the endpoint, 401) close the source
// permanently, so those are reconnected manually with capped exponential
// backoff. The channel is a background enhancement: it must never toast
// or spam the console when the backend lacks the endpoint.

const INITIAL_BACKOFF_MS = 1_000
const MAX_BACKOFF_MS = 30_000

// Query-key prefixes covering every query derived from files on disk.
// TanStack prefix matching makes e.g. 'flowgroups' cover
// ['flowgroups', pipeline]; note 'flowgroup' does NOT prefix-match
// 'flowgroup-related-files' (element-wise equality), so each key is
// listed explicitly.
//
// 'dep-graph' is deliberately ABSENT: the dependency graph is served
// stale-tolerant. A graph-relevant edit rides its own `graph-stale` event
// (handled below) which flips a client flag + surfaces a manual Refresh,
// instead of refetching the whole graph on every keystroke.
const FILE_CHANGED_KEYS = [
  'files',
  'flowgroups',
  'flowgroup',
  'flowgroup-related-files',
  'flowgroup-resolved',
  'pipelines',
  'pipeline',
  'pipeline-flowgroups',
  'tables',
  'execution-order',
  'circular-deps',
  'stats',
] as const

/**
 * Extract the JSON payload of a push event. Returns `null` (rather than
 * throwing) for non-message events, non-string data, and malformed JSON
 * — the channel ignores anything it cannot parse.
 */
function parseEventData(event: Event): unknown {
  if (!(event instanceof MessageEvent)) return null
  const data: unknown = event.data
  if (typeof data !== 'string') return null
  try {
    return JSON.parse(data) as unknown
  } catch {
    return null
  }
}

function isFileChangedPayload(value: unknown): value is FileChangedPayload {
  return (
    typeof value === 'object' &&
    value !== null &&
    'paths' in value &&
    Array.isArray(value.paths)
  )
}

function isRunUpdatedPayload(value: unknown): value is RunUpdatedPayload {
  return (
    typeof value === 'object' &&
    value !== null &&
    'run_id' in value &&
    typeof value.run_id === 'string' &&
    'kind' in value &&
    (value.kind === 'validate' || value.kind === 'generate') &&
    'status' in value &&
    (value.status === 'running' ||
      value.status === 'completed' ||
      value.status === 'failed')
  )
}

/**
 * Mount the push channel. Call exactly once, from Layout. Renders
 * nothing and returns nothing — its only output is query invalidation.
 */
export function usePushChannel(): void {
  const queryClient = useQueryClient()

  useEffect(() => {
    let source: EventSource | null = null
    let reconnectTimer: number | null = null
    let backoffMs = INITIAL_BACKOFF_MS
    // Set on any error/disconnect; the next successful open then
    // invalidates everything (see module comment on missed events).
    let hadDisconnect = false
    let disposed = false
    let loggedClosed = false

    const handleFileChanged = (event: Event) => {
      const payload = parseEventData(event)
      if (!isFileChangedPayload(payload)) return
      for (const key of FILE_CHANGED_KEYS) {
        void queryClient.invalidateQueries({ queryKey: [key] })
      }
      // Per-path raw-content queries (the Config UI's useConfigFile):
      // exact keys, so an edit to one file never refetches every open one.
      for (const path of payload.paths) {
        if (typeof path !== 'string') continue
        void queryClient.invalidateQueries({ queryKey: ['file-content', path] })
      }
    }

    const handleGraphStale = () => {
      // Serve-stale: flip the client flag and let the toolbar offer a manual
      // Refresh. Deliberately does NOT refetch the graph. The payload (the
      // changed paths) is unused — the flag is app-wide, not per-path.
      useGraphStalenessStore.getState().markStale()
    }

    const handleRunUpdated = (event: Event) => {
      const payload = parseEventData(event)
      if (!isRunUpdatedPayload(payload)) return
      // The run-history page arrives in a later phase; invalidating a
      // key with no active queries is harmless.
      void queryClient.invalidateQueries({ queryKey: ['run-history'] })
      if (payload.kind === 'generate' && payload.status === 'completed') {
        // Mirrors the existing post-generate invalidation for runs that
        // completed in another tab / on the server side.
        void queryClient.invalidateQueries({ queryKey: ['files'] })
        void queryClient.invalidateQueries({ queryKey: ['dep-graph'] })
      }
    }

    const connect = () => {
      if (disposed) return
      const next = createPushSource()
      if (next === null) return
      source = next

      next.onopen = () => {
        backoffMs = INITIAL_BACKOFF_MS
        if (hadDisconnect) {
          hadDisconnect = false
          void queryClient.invalidateQueries()
        }
      }

      next.addEventListener(FILE_CHANGED_EVENT, handleFileChanged)
      next.addEventListener(GRAPH_STALE_EVENT, handleGraphStale)
      next.addEventListener(RUN_UPDATED_EVENT, handleRunUpdated)

      next.onerror = () => {
        hadDisconnect = true
        // readyState CONNECTING means a network drop: EventSource is
        // already retrying on its own. CLOSED means a permanent failure
        // (HTTP error response) — reconnect manually with backoff.
        if (next.readyState !== EventSource.CLOSED) return
        next.close()
        if (disposed) return
        if (!loggedClosed) {
          loggedClosed = true
          console.debug('[push] /api/events closed; retrying with backoff')
        }
        reconnectTimer = window.setTimeout(connect, backoffMs)
        backoffMs = Math.min(backoffMs * 2, MAX_BACKOFF_MS)
      }
    }

    connect()

    // StrictMode-safe: cleanup closes this effect run's source (and any
    // pending reconnect) before the re-run opens a fresh one, so two
    // live sources never coexist.
    return () => {
      disposed = true
      if (reconnectTimer !== null) window.clearTimeout(reconnectTimer)
      source?.close()
    }
  }, [queryClient])
}
