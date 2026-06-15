import { useCallback, useEffect, useRef, useState } from 'react'
import { useQueryClient } from '@tanstack/react-query'
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
 * several frames.
 */
function splitLines(buffer: string): { lines: string[]; rest: string } {
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
 */
function parseLine(rawLine: string): StreamFrame | null {
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

export function useEventStream(): UseEventStreamResult {
  const queryClient = useQueryClient()
  const [isRunning, setIsRunning] = useState(false)
  const [frames, setFrames] = useState<StreamFrame[]>([])
  const [error, setError] = useState<Error | ErrorFrame | null>(null)

  const abortRef = useRef<AbortController | null>(null)
  // Snapshot of run flag readable inside the async loop without stale
  // closures; mirrors `isRunning` but is synchronous.
  const runningRef = useRef(false)

  const abort = useCallback(() => {
    abortRef.current?.abort()
  }, [])

  const start = useCallback(
    (options: StartOptions, callbacks?: StreamCallbacks) => {
      // v1: idempotent while running — ignore re-entrant starts.
      if (runningRef.current) return

      const controller = new AbortController()
      abortRef.current = controller
      runningRef.current = true
      setIsRunning(true)
      setFrames([])
      setError(null)

      const { path, ...body } = options
      const { onFrame, onError, onDone } = callbacks ?? {}

      const emitError = (err: Error | ErrorFrame) => {
        setError(err)
        onError?.(err)
      }

      const run = async () => {
        let sawError = false
        let sawGenerationSuccess = false
        try {
          const response = await startStream(path, body, controller.signal)
          const stream = response.body
          if (stream === null) {
            throw new Error('Stream response had no body')
          }

          const reader = stream.getReader()
          const decoder = new TextDecoder()
          let buffer = ''

          const handleLine = (rawLine: string) => {
            let frame: StreamFrame | null
            try {
              frame = parseLine(rawLine)
            } catch {
              // Skip malformed JSON lines rather than aborting the run;
              // the server contract is one JSON object per line.
              return
            }
            if (frame === null) return

            setFrames((prev) => [...prev, frame])
            onFrame?.(frame)

            if (frame.type === 'error') {
              sawError = true
              emitError(frame)
            } else if (
              frame.type === 'GenerationCompleted' &&
              frame.response.success
            ) {
              sawGenerationSuccess = true
            }
          }

          let done = false
          while (!done) {
            const result = await reader.read()
            done = result.done
            if (result.value !== undefined) {
              buffer += decoder.decode(result.value, { stream: true })
              const split = splitLines(buffer)
              buffer = split.rest
              for (const rawLine of split.lines) {
                handleLine(rawLine)
              }
            }
          }

          // Flush any trailing buffered line (stream may end without a
          // final newline).
          buffer += decoder.decode()
          if (buffer.trim() !== '') {
            handleLine(buffer)
          }
        } catch (err) {
          if (controller.signal.aborted) {
            // Caller-initiated abort / unmount — not a real failure.
            return
          }
          sawError = true
          const normalized =
            err instanceof ApiError || err instanceof Error
              ? err
              : new Error('Event stream failed')
          emitError(normalized)
        } finally {
          const aborted = controller.signal.aborted
          // Only invalidate on a clean, successful generate run.
          if (sawGenerationSuccess && !sawError && !aborted) {
            queryClient.invalidateQueries({ queryKey: ['files'] })
            queryClient.invalidateQueries({ queryKey: ['dep-graph'] })
          }
          // Guard against a superseded controller (StrictMode/restart):
          // only the latest run clears the shared running state.
          if (abortRef.current === controller) {
            abortRef.current = null
            runningRef.current = false
            setIsRunning(false)
          }
          onDone?.({ aborted })
        }
      }

      void run()
    },
    [queryClient],
  )

  // StrictMode-safe: abort any in-flight stream on unmount.
  useEffect(() => {
    return () => {
      abortRef.current?.abort()
    }
  }, [])

  return { start, abort, isRunning, frames, error }
}
