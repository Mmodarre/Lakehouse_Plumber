import { useCallback, useEffect, useRef } from 'react'
import { useQueryClient } from '@tanstack/react-query'
import { startAssistantChat } from '../api/assistant'
import { useAssistantStore } from '../store/assistantStore'
import type { AssistantFrame } from '../types/assistant'
import { parseLine, splitLines } from './useEventStream'

// ── useAssistantStream — chat NDJSON transport hook ──────────
//
// Mirrors `useEventStream`'s fetch + ReadableStream lifecycle for the
// assistant chat endpoint (`POST /api/assistant/chat`), one stream PER TAB:
// every decoded frame is routed into that tab's conversation via
// `assistantStore.applyFrame(tabKey, …)` — the store is the reducer, this
// hook models nothing itself. Streams on different tabs run concurrently;
// a per-tab running set guards double-send on one tab.
//
// Rekey routing: the request sends the tab key as `session_id` (a draft key
// is unknown server-side, so the backend mints a fresh session); when the
// stream's `session` frame reports a different id, the tab is re-keyed and
// every SUBSEQUENT frame of this stream lands on the new key — the local
// `key` binding follows the tab, surviving the rename.
//
// Failure split: a terminal `error` frame reaches the tab's conversation
// through `applyFrame`; anything that prevents/kills the transport (gate
// 409s on open, network drop, parse-loop crash) goes through
// `failTransport`. A caller-initiated abort (panel unmount) is neither —
// the turn continues server-side and rehydration shows it.
//
// The stop button does NOT abort these streams: it POSTs /interrupt (see
// `useInterruptAssistant`) and the backend ends the turn with an
// `interrupted` frame.

export interface UseAssistantStreamResult {
  /** Send one user message on a tab. No-op while that tab is streaming. */
  send: (tabKey: string, message: string) => void
  /** Abort one tab's in-flight fetch (teardown — NOT the stop button). */
  abort: (tabKey: string) => void
  /** True while the ACTIVE tab's stream is open (from the store). */
  isStreaming: boolean
}

export function useAssistantStream(): UseAssistantStreamResult {
  const queryClient = useQueryClient()
  const isStreaming = useAssistantStore((s) =>
    s.activeTabKey !== null
      ? (s.conversations[s.activeTabKey]?.streaming ?? false)
      : false,
  )

  const controllersRef = useRef(new Map<string, AbortController>())
  // Synchronous per-tab re-entrancy guard (state updates are async).
  const runningRef = useRef(new Set<string>())

  const abort = useCallback((tabKey: string) => {
    controllersRef.current.get(tabKey)?.abort()
  }, [])

  const send = useCallback(
    (tabKey: string, message: string) => {
      if (runningRef.current.has(tabKey)) return

      const controller = new AbortController()
      controllersRef.current.set(tabKey, controller)
      runningRef.current.add(tabKey)

      // Follows the tab across a rekey; frames always land on the tab's
      // CURRENT key.
      let key = tabKey

      const { beginTurn, permissionMode } = useAssistantStore.getState()
      beginTurn(key, message)

      const run = async () => {
        try {
          const response = await startAssistantChat(
            // Mode read at send time (per-turn); the tab key doubles as the
            // session target — unknown (draft) keys mint a fresh session.
            { message, permission_mode: permissionMode, session_id: key },
            controller.signal,
          )
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
            const store = useAssistantStore.getState()
            if (frame.type === 'session' && frame.session_id !== key) {
              // The backend bound this tab to a (new) real session: re-key
              // the tab and move this stream's bookkeeping with it.
              const fromKey = key
              key = frame.session_id
              store.rekeyTab(fromKey, key)
              runningRef.current.delete(fromKey)
              runningRef.current.add(key)
              if (controllersRef.current.get(fromKey) === controller) {
                controllersRef.current.delete(fromKey)
                controllersRef.current.set(key, controller)
              }
            }
            store.applyFrame(key, frame)
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
          useAssistantStore
            .getState()
            .failTransport(
              key,
              err instanceof Error ? err : new Error('Assistant stream failed'),
            )
        } finally {
          // Guard against a superseded controller (StrictMode/restart):
          // only the latest run may close its tab's streaming state.
          if (controllersRef.current.get(key) === controller) {
            controllersRef.current.delete(key)
            runningRef.current.delete(key)
            useAssistantStore.getState().finishStream(key)
          }
          // The turn may have provisioned a session / set a title / changed
          // last-used bookkeeping — keep the status card and tab strip fresh.
          void queryClient.invalidateQueries({ queryKey: ['assistant-status'] })
          void queryClient.invalidateQueries({ queryKey: ['assistant-sessions'] })
        }
      }

      void run()
    },
    [queryClient],
  )

  // StrictMode-safe: abort every in-flight stream on unmount.
  useEffect(() => {
    const controllers = controllersRef.current
    return () => {
      for (const controller of controllers.values()) {
        controller.abort()
      }
    }
  }, [])

  return { send, abort, isStreaming }
}
