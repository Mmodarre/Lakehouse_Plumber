/**
 * SSE stream hook — manages EventSource lifecycle and dispatches
 * OpenCode events to the chat store.
 *
 * Uses the user-scoped /api/ai/events endpoint (single connection for
 * all sessions). Events are dispatched to the correct session's message
 * slice via the sessionID in each event payload.
 *
 * Key SSE event types handled:
 * - message.updated      → ensure assistant message exists in store
 * - message.part.updated → append streaming text delta or replace full part
 * - session.idle          → mark streaming as complete
 * - session.updated       → update session title in tab bar
 */

import { useEffect, useRef } from 'react'
import { useChatStore } from '../store/chatStore'
import { connectAIUserEventStream, getSessionMessages, fetchPendingQuestions } from '../api/ai'
import { mapRawMessage } from '../utils/messageMapping'
import type { ChatMessagePart, SSEMessagePartUpdated, SSEMessageUpdated, SSEQuestionAsked, SSESessionError, SSESessionIdle, SSESessionUpdated } from '../types/chat'

/**
 * Connect to the user-scoped SSE stream when connected.
 *
 * @param connected — true when the AI backend is available and connected
 */
export function useSSEStream(connected: boolean) {
  const eventSourceRef = useRef<EventSource | null>(null)
  const reconnectTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null)
  const watchdogTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null)
  const wasReconnectingRef = useRef(false)
  // Per-session tracking
  const userMessageIdsRef = useRef<Map<string, Set<string>>>(new Map())
  const pendingPartsRef = useRef<Map<string, Map<string, SSEMessagePartUpdated[]>>>(new Map())

  const {
    ensureAssistantMessage,
    appendMessageDelta,
    replaceMessagePart,
    finalizeMessage,
    addMessageToSession,
    setSessionStreaming,
    setStreamingStalled,
    setConnectionStatus,
    incrementUnread,
    updateSessionTitle,
  } = useChatStore()

  useEffect(() => {
    if (!connected) {
      // Not connected — close any existing stream
      if (eventSourceRef.current) {
        eventSourceRef.current.close()
        eventSourceRef.current = null
      }
      return
    }

    // Close previous stream if re-connecting
    if (eventSourceRef.current) {
      eventSourceRef.current.close()
    }

    // Reset tracking
    userMessageIdsRef.current.clear()
    pendingPartsRef.current.clear()

    const es = connectAIUserEventStream()
    eventSourceRef.current = es

    es.onopen = () => {
      if (reconnectTimerRef.current) {
        clearTimeout(reconnectTimerRef.current)
        reconnectTimerRef.current = null
      }
      setConnectionStatus('connected')

      // If this was a reconnect, catch up on active session messages
      if (wasReconnectingRef.current) {
        wasReconnectingRef.current = false
        const activeId = useChatStore.getState().activeSessionId
        if (activeId) catchUpMessages(activeId)
      }

      // Poll for any pending questions whose question.asked event we missed
      catchUpPendingQuestions()
    }

    es.onerror = () => {
      setConnectionStatus('reconnecting')
      wasReconnectingRef.current = true

      if (!reconnectTimerRef.current) {
        reconnectTimerRef.current = setTimeout(() => {
          reconnectTimerRef.current = null
          const state = useChatStore.getState()
          const activeId = state.activeSessionId
          if (activeId && (state.sessionStreaming[activeId] ?? false)) {
            addMessageToSession(activeId, {
              id: `conn-error-${Date.now()}`,
              role: 'assistant',
              parts: [{ type: 'text', text: 'Connection lost. Attempting to reconnect...' }],
              timestamp: Date.now(),
              isStreaming: false,
            })
            setSessionStreaming(activeId, false)
            finalizeAllStreamingMessages(activeId)
          }
        }, 10_000)
      }
    }

    es.onmessage = (event: MessageEvent) => {
      let data: { type: string; properties: Record<string, unknown> }
      try {
        data = JSON.parse(event.data)
      } catch {
        return
      }

      const sessionId = extractSessionId(data)
      if (!sessionId) return // Can't dispatch without a session ID

      // session.updated applies to availableSessions too, so always process it.
      // For other event types, skip if the session is not in open tabs
      // to prevent phantom state accumulation for closed tabs.
      if (data.type !== 'session.updated') {
        const openSessions = useChatStore.getState().sessions
        if (!openSessions.some((s) => s.id === sessionId)) return
      }

      switch (data.type) {
        case 'message.updated':
          handleMessageUpdated(sessionId, data as SSEMessageUpdated)
          break
        case 'message.part.updated':
          handleMessagePartUpdated(sessionId, data as SSEMessagePartUpdated)
          break
        case 'session.idle':
          handleSessionIdle(sessionId, data as SSESessionIdle)
          break
        case 'session.error':
          handleSessionError(sessionId, data as SSESessionError)
          break
        case 'session.updated':
          handleSessionUpdated(data as SSESessionUpdated)
          break
        case 'question.asked':
          handleQuestionAsked(data as SSEQuestionAsked)
          break
        case 'question.replied':
        case 'question.rejected':
          break
        default:
          break
      }
    }

    return () => {
      es.close()
      eventSourceRef.current = null
      if (reconnectTimerRef.current) {
        clearTimeout(reconnectTimerRef.current)
        reconnectTimerRef.current = null
      }
      clearWatchdog()
    }
  }, [connected]) // eslint-disable-line react-hooks/exhaustive-deps

  // ── Per-session tracking helpers ───────────────────────────────

  function getUserMessageIds(sessionId: string): Set<string> {
    let set = userMessageIdsRef.current.get(sessionId)
    if (!set) {
      set = new Set()
      userMessageIdsRef.current.set(sessionId, set)
    }
    return set
  }

  function getPendingParts(sessionId: string): Map<string, SSEMessagePartUpdated[]> {
    let map = pendingPartsRef.current.get(sessionId)
    if (!map) {
      map = new Map()
      pendingPartsRef.current.set(sessionId, map)
    }
    return map
  }

  // ── Event handlers ──────────────────────────────────────────

  function handleMessageUpdated(sessionId: string, event: SSEMessageUpdated) {
    const info = event.properties.info

    if (info.role === 'user') {
      getUserMessageIds(sessionId).add(info.id)
      getPendingParts(sessionId).delete(info.id)
      return
    }

    if (info.role === 'assistant') {
      ensureAssistantMessage(info.id, sessionId)
      setSessionStreaming(sessionId, true)
      resetWatchdog()
      incrementUnread(sessionId)

      // Flush any part events that arrived before this message.updated
      const pending = getPendingParts(sessionId)
      const buffered = pending.get(info.id)
      if (buffered) {
        pending.delete(info.id)
        for (const partEvent of buffered) {
          processPartEvent(sessionId, partEvent)
        }
      }

      if (info.time.completed) {
        finalizeMessage(sessionId, info.id)
      }
    }
  }

  function handleMessagePartUpdated(sessionId: string, event: SSEMessagePartUpdated) {
    const messageId = event.properties.part.messageID

    if (getUserMessageIds(sessionId).has(messageId)) return

    const msgs = useChatStore.getState().sessionMessages[sessionId] ?? []
    if (!msgs.some((m) => m.id === messageId)) {
      const pending = getPendingParts(sessionId)
      const buf = pending.get(messageId) ?? []
      buf.push(event)
      pending.set(messageId, buf)
      return
    }

    processPartEvent(sessionId, event)
  }

  function processPartEvent(sessionId: string, event: SSEMessagePartUpdated) {
    const { part, delta } = event.properties
    const messageId = part.messageID
    const partType = part.type
    const partId = part.id

    setSessionStreaming(sessionId, true)
    resetWatchdog()

    if ((partType === 'text' || partType === 'reasoning') && delta) {
      appendMessageDelta(sessionId, messageId, partId, partType, delta)
      return
    }

    if (partType === 'tool') {
      const chatPart = mapToolPart(part)
      replaceMessagePart(sessionId, messageId, partId, chatPart)
      return
    }

    if (partType === 'text' || partType === 'reasoning') {
      const chatPart: ChatMessagePart = {
        type: partType,
        text: part.text ?? '',
      }
      replaceMessagePart(sessionId, messageId, partId, chatPart)
      return
    }
  }

  function handleSessionIdle(sessionId: string, _event: SSESessionIdle) {
    setSessionStreaming(sessionId, false)
    clearWatchdog()
    getPendingParts(sessionId).clear()
    finalizeAllStreamingMessages(sessionId)
  }

  function handleSessionError(sessionId: string, event: SSESessionError) {
    const errorText = event.properties.error || 'Unknown AI error'
    addMessageToSession(sessionId, {
      id: `error-${Date.now()}`,
      role: 'assistant',
      parts: [{ type: 'text', text: `Error: ${errorText}` }],
      timestamp: Date.now(),
      isStreaming: false,
    })
    setSessionStreaming(sessionId, false)
    clearWatchdog()
    getPendingParts(sessionId).clear()
    finalizeAllStreamingMessages(sessionId)
  }

  function handleSessionUpdated(event: SSESessionUpdated) {
    const info = event.properties.info
    if (info.id && info.title) {
      updateSessionTitle(info.id, info.title)
    }
  }

  function handleQuestionAsked(event: SSEQuestionAsked) {
    const { id: requestId, sessionID: qSessionId } = event.properties
    if (qSessionId && requestId) {
      useChatStore.getState().setPendingQuestionId(qSessionId, requestId)
    }
  }

  function resetWatchdog() {
    if (watchdogTimerRef.current) {
      clearTimeout(watchdogTimerRef.current)
    }
    watchdogTimerRef.current = setTimeout(() => {
      setStreamingStalled(true)
    }, 30_000)
    setStreamingStalled(false)
  }

  function clearWatchdog() {
    if (watchdogTimerRef.current) {
      clearTimeout(watchdogTimerRef.current)
      watchdogTimerRef.current = null
    }
    setStreamingStalled(false)
  }

  function finalizeAllStreamingMessages(sessionId: string) {
    const msgs = useChatStore.getState().sessionMessages[sessionId] ?? []
    for (const msg of msgs) {
      if (msg.isStreaming) {
        finalizeMessage(sessionId, msg.id)
      }
    }
  }

  async function catchUpPendingQuestions() {
    try {
      const pending = await fetchPendingQuestions()
      const store = useChatStore.getState()
      for (const q of pending) {
        if (q.sessionID && q.id && !store.pendingQuestionIds[q.sessionID]) {
          store.setPendingQuestionId(q.sessionID, q.id)
        }
      }
    } catch {
      // Best-effort
    }
  }

  async function catchUpMessages(sid: string) {
    try {
      const rawMessages = await getSessionMessages(sid)
      const chatMessages = (rawMessages as Array<Record<string, unknown>>).map(mapRawMessage)
      useChatStore.getState().setSessionMessages(sid, chatMessages)
    } catch {
      // Best-effort catch-up
    }
  }
}

// ── Helpers ────────────────────────────────────────────────────

function extractSessionId(event: { type: string; properties: Record<string, unknown> }): string | null {
  const props = event.properties
  if (typeof props.sessionID === 'string') return props.sessionID
  if (props.info && typeof (props.info as Record<string, unknown>).sessionID === 'string') {
    return (props.info as Record<string, unknown>).sessionID as string
  }
  if (props.part && typeof (props.part as Record<string, unknown>).sessionID === 'string') {
    return (props.part as Record<string, unknown>).sessionID as string
  }
  return null
}

/**
 * Map an OpenCode ToolPart to our ChatMessagePart.
 */
function mapToolPart(part: SSEMessagePartUpdated['properties']['part']): ChatMessagePart {
  const toolName = part.tool
  const stateObj = part.state
  const status = stateObj?.status ?? 'pending'
  const input = stateObj?.input
  const title = stateObj?.title

  const mapped: ChatMessagePart = {
    type: 'tool-call',
    toolName: toolName ?? undefined,
    toolArgs: input ?? undefined,
    toolState: status,
  }

  if (title && mapped.toolArgs) {
    mapped.toolArgs = { ...mapped.toolArgs, _title: title }
  } else if (title && !mapped.toolArgs) {
    mapped.toolArgs = { _title: title }
  }

  if (status === 'completed' && stateObj?.output !== undefined) {
    mapped.toolResult = typeof stateObj.output === 'string'
      ? stateObj.output
      : JSON.stringify(stateObj.output)
  }

  if (status === 'error') {
    mapped.isError = true
    mapped.toolError = stateObj?.error ?? undefined
  }

  return mapped
}
