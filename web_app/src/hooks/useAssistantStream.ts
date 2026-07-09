import { useCallback, useEffect, useRef } from 'react'
import { useQueryClient } from '@tanstack/react-query'
import { startAssistantChat } from '../api/assistant'
import { useAssistantStore } from '../store/assistantStore'
import type { AssistantFrame } from '../types/assistant'
import { parseLine, splitLines } from './useEventStream'

// ── useAssistantStream — chat NDJSON transport hook ──────────
//
// Mirrors `useEventStream`'s fetch + ReadableStream lifecycle for the
// assistant chat endpoint (`POST /api/assistant/chat`), but feeds every
// decoded frame straight into `assistantStore.applyFrame` — the store is
// the single reducer, so this hook models nothing itself.
//
// Failure split: a terminal `error` frame (e.g. LHP-GEN-902 daemon lost)
// reaches the store through `applyFrame`; anything that prevents/kills
// the transport (gate 409s on open, network drop, parse-loop crash) goes
// through `failTransport`. A caller-initiated abort (panel unmount) is
// neither — the turn continues server-side and rehydration shows it.
//
// The stop button does NOT abort this stream: it POSTs /interrupt (see
// `useInterruptAssistant`) and the backend ends the turn with an
// `interrupted` frame.

export interface UseAssistantStreamResult {
  /** Send one user message. No-op while a turn is already streaming. */
  send: (message: string) => void
  /** Abort the in-flight fetch (unmount/teardown — NOT the stop button). */
  abort: () => void
  /** True while a turn's stream is open (from the store). */
  isStreaming: boolean
}

export function useAssistantStream(): UseAssistantStreamResult {
  const queryClient = useQueryClient()
  const isStreaming = useAssistantStore((s) => s.streaming)

  const abortRef = useRef<AbortController | null>(null)
  // Synchronous re-entrancy guard (state updates are async).
  const runningRef = useRef(false)

  const abort = useCallback(() => {
    abortRef.current?.abort()
  }, [])

  const send = useCallback(
    (message: string) => {
      if (runningRef.current) return

      const controller = new AbortController()
      abortRef.current = controller
      runningRef.current = true

      const { beginTurn, applyFrame, failTransport, finishStream } =
        useAssistantStore.getState()
      beginTurn(message)

      const run = async () => {
        try {
          const response = await startAssistantChat({ message }, controller.signal)
          const stream = response.body
          if (stream === null) {
            throw new Error('Chat stream response had no body')
          }

          const reader = stream.getReader()
          const decoder = new TextDecoder()
          let buffer = ''

          const handleLine = (rawLine: string) => {
            let frame: AssistantFrame | null
            try {
              frame = parseLine(rawLine) as AssistantFrame | null
            } catch {
              // One JSON object per line is the contract; skip a malformed
              // line rather than killing the turn.
              return
            }
            if (frame === null) return
            applyFrame(frame)
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

          // Flush a trailing frame if the stream ended without a newline.
          buffer += decoder.decode()
          if (buffer.trim() !== '') {
            handleLine(buffer)
          }
        } catch (err) {
          if (controller.signal.aborted) {
            // Unmount/teardown — the turn continues server-side.
            return
          }
          failTransport(
            err instanceof Error ? err : new Error('Assistant stream failed'),
          )
        } finally {
          // Guard against a superseded controller (StrictMode/restart):
          // only the latest run may close the shared streaming state.
          if (abortRef.current === controller) {
            abortRef.current = null
            runningRef.current = false
            finishStream({ aborted: controller.signal.aborted })
          }
          // The turn may have provisioned a session / changed its
          // last-used bookkeeping — keep the status card fresh.
          void queryClient.invalidateQueries({ queryKey: ['assistant-status'] })
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

  return { send, abort, isStreaming }
}
