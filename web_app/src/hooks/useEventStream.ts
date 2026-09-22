import { useCallback, useEffect, useRef, useState } from 'react'
import { useQueryClient, type QueryClient } from '@tanstack/react-query'
import { startStream } from '../api/stream'
import type { StreamBody, StreamPath } from '../api/stream'
import { ApiError } from '../api/client'
import type { ErrorFrame, StreamFrame } from '../types/api'

// ── useEventStream — NDJSON/SSE transport hook ───────────────
//
// A transport-focused hook for the two server-sent run streams
// (`POST /api/validate/stream`, `POST /api/generate/stream`). It owns
// the fetch + ReadableStream lifecycle and decodes the byte stream into
// typed `StreamFrame`s, exposing them both as accumulating state (for a
// simple panel) and via per-frame callbacks (for a store built on top).
//
// It deliberately does NOT model run results, phases, or progress —
// that reshaping lives one layer up in the runStore. This hook's job is
// purely: open the stream, parse frames, surface them, and tear down.
//
// The endpoints are POST-with-body, so the only viable client is
// `fetch` + manual line parsing (the SSE browser API only does GET).

export interface StartOptions extends StreamBody {
  /** Which run stream to open. */
  path: StreamPath
}

export interface StreamCallbacks {
  /** Invoked once per decoded frame, in arrival order. */
  onFrame?: (frame: StreamFrame) => void
  /**
   * Invoked once if the run fails before/instead of terminating
   * cleanly: an `ApiError` (non-2xx open, see `startStream`), a
   * transport/parse error, or a terminal `ErrorFrame` from the server.
   */
  onError?: (error: Error | ErrorFrame) => void
  /**
   * Invoked exactly once when the stream finishes — whether it ended
   * normally, errored, or was aborted. `aborted` distinguishes a
   * caller-initiated `abort()` / unmount from a natural end.
   */
  onDone?: (info: { aborted: boolean }) => void
}

export interface UseEventStreamResult {
  /**
   * Open a stream. No-op if a stream is already running (v1: a second
   * `start()` while running is ignored — call `abort()` first to
   * restart). Returns immediately; consume results via callbacks or the
   * accumulating `frames` state.
   */
  start: (options: StartOptions, callbacks?: StreamCallbacks) => void
  /** Abort the in-flight fetch/stream, if any. Safe to call when idle. */
  abort: () => void
  /** True while a stream is open and being read. */
  isRunning: boolean
  /** All frames decoded during the current/last run, in arrival order. */
  frames: StreamFrame[]
  /**
   * Set when the run failed: an `Error` (transport/HTTP) or a terminal
   * `ErrorFrame`. Cleared at the start of each new run.
   */
  error: Error | ErrorFrame | null
}

/**
 * Split a decoded text chunk into complete lines, returning the
 * still-incomplete trailing fragment to be carried into the next chunk.
 * A frame may be split across network chunks, and a chunk may contain
 * several frames. Exported for unit tests only.
 */
export function splitLines(buffer: string): { lines: string[]; rest: string } {
  const parts = buffer.split('\n')
  // The last element is either an empty string (buffer ended on '\n')
  // or a partial line that has not yet been terminated.
  const rest = parts.pop() ?? ''
  return { lines: parts, rest }
}

/**
 * Parse one raw line into a frame. Tolerates both bare NDJSON
 * (`{...}`) and `data:`-prefixed SSE lines (`data: {...}`). Returns
 * `null` for empty lines, SSE comments (`:`-prefixed), and other
 * non-data SSE fields (`event:`, `id:`, `retry:`) that carry no JSON.
 * Exported for unit tests only.
 */
export function parseLine(rawLine: string): StreamFrame | null {
  const line = rawLine.trim()
  if (line === '') return null
  if (line.startsWith(':')) return null // SSE comment

  let payload = line
  if (payload.startsWith('data:')) {
    payload = payload.slice('data:'.length).trim()
    if (payload === '') return null
  } else if (/^(event|id|retry):/.test(payload)) {
    // Non-data SSE field — no JSON body.
    return null
  }

  return JSON.parse(payload) as StreamFrame
}

// Module-level handle on the single in-flight run's AbortController. The run
// is one-at-a-time (backend asyncio.Lock), so one slot suffices. It lets a
// Stop control mounted OUTSIDE the launching component (the bottom Run panel,
// which cannot reach the CommandBar's per-instance abortRef) cooperatively
// abort the active run — the only cross-component abort path.
let activeStreamController: AbortController | null = null

/**
 * Cooperatively abort whichever run stream is currently in flight, from
 * anywhere (no hook instance / no props threading required). No-op when idle.
 * "Cooperative" = the backend finishes the current flowgroup before stopping;
 * the recorder marks the run failed if no terminal frame arrived.
 */
export function abortActiveStream(): void {
  activeStreamController?.abort()
}

/** Transport lifetime belongs to the operation, independent of any launching view. */
export function startEventStream(
  options: StartOptions,
  callbacks: StreamCallbacks,
  queryClient: QueryClient,
): AbortController | null {
  if (activeStreamController) return null
  const controller = new AbortController()
  activeStreamController = controller
  const { path, ...body } = options
  void (async () => {
    let sawError = false
    let sawGenerationSuccess = false
    let reader: ReadableStreamDefaultReader<Uint8Array> | undefined
    const emitError = (error: Error | ErrorFrame) => {
      sawError = true
      callbacks.onError?.(error)
    }
    try {
      const response = await startStream(path, body, controller.signal)
      if (!response.body) throw new Error('Stream response had no body')
      reader = response.body.getReader()
      // Also cancel synthetic/test readers which do not observe fetch's signal.
      const cancelReader = () => { void reader?.cancel().catch(() => {}) }
      controller.signal.addEventListener('abort', cancelReader, { once: true })
      const decoder = new TextDecoder()
      let buffer = ''
      const handleLine = (line: string) => {
        if (controller.signal.aborted) return
        let frame: StreamFrame | null
        try { frame = parseLine(line) } catch { return }
        if (!frame) return
        callbacks.onFrame?.(frame)
        if (frame.type === 'error') emitError(frame)
        if (frame.type === 'GenerationCompleted' && frame.response.success) {
          sawGenerationSuccess = true
        }
      }
      try {
        while (!controller.signal.aborted) {
          const result = await reader.read()
          if (result.value) {
            buffer += decoder.decode(result.value, { stream: true })
            const split = splitLines(buffer)
            buffer = split.rest
            split.lines.forEach(handleLine)
          }
          if (result.done) break
        }
        buffer += decoder.decode()
        if (buffer.trim()) handleLine(buffer)
      } finally {
        controller.signal.removeEventListener('abort', cancelReader)
      }
    } catch (error) {
      if (!controller.signal.aborted) {
        emitError(error instanceof ApiError || error instanceof Error
          ? error : new Error('Event stream failed'))
      }
    } finally {
      reader?.releaseLock()
      const aborted = controller.signal.aborted
      if (sawGenerationSuccess && !sawError && !aborted) {
        for (const key of ['files', 'dep-graph', 'flowgroup-related', 'flowgroup-related-files', 'file-content', 'file-exists', 'tables', 'lineage']) {
          void queryClient.invalidateQueries({ queryKey: [key] })
        }
      }
      void queryClient.invalidateQueries({ queryKey: ['run-history'] })
      if (activeStreamController === controller) activeStreamController = null
      callbacks.onDone?.({ aborted })
    }
  })()
  return controller
}

export function useEventStream(): UseEventStreamResult {
  const queryClient = useQueryClient()
  const [isRunning, setIsRunning] = useState(false)
  const [frames, setFrames] = useState<StreamFrame[]>([])
  const [error, setError] = useState<Error | ErrorFrame | null>(null)
  const abortRef = useRef<AbortController | null>(null)
  const mounted = useRef(true)
  const abort = useCallback(() => abortRef.current?.abort(), [])
  const start = useCallback((options: StartOptions, callbacks?: StreamCallbacks) => {
    const controller = startEventStream(options, {
      onFrame: (frame) => {
        if (mounted.current) setFrames((previous) => [...previous, frame])
        callbacks?.onFrame?.(frame)
      },
      onError: (failure) => {
        if (mounted.current) setError(failure)
        callbacks?.onError?.(failure)
      },
      onDone: (info) => {
        if (abortRef.current === controller) {
          abortRef.current = null
          if (mounted.current) setIsRunning(false)
        }
        callbacks?.onDone?.(info)
      },
    }, queryClient)
    if (!controller) return
    abortRef.current = controller
    setIsRunning(true)
    setFrames([])
    setError(null)
  }, [queryClient])
  useEffect(() => {
    mounted.current = true
    return () => {
      mounted.current = false
      abortRef.current?.abort()
    }
  }, [])
  return { start, abort, isRunning, frames, error }
}
