/**
 * Core chat hook — send/stop message orchestration.
 *
 * With the SSE refactor, sendMessage() is now fire-and-forget:
 * 1. POST /api/ai/session/{id}/message → returns immediately
 * 2. SSE events (via useSSEStream) update the store incrementally
 *
 * The hook no longer blocks on the LLM response.
 *
 * Multi-session: creates sessions with the store's defaultMode,
 * and targets messages to the correct session via addMessageToSession.
 */

import { useCallback, useRef } from 'react'
import { useChatStore } from '../store/chatStore'
import { useUIStore } from '../store/uiStore'
import { createAISession, sendAIMessage, cancelAIGeneration, generateSessionTitle } from '../api/ai'
import type { ChatMessage } from '../types/chat'

let _messageCounter = 0
function nextMessageId(): string {
  return `msg-${Date.now()}-${++_messageCounter}`
}

export function useChat() {
  const {
    activeSessionId,
    setActiveSession,
    addSession,
    addMessageToSession,
    setSessionStreaming,
    chatContext,
    defaultMode,
    sessionStreaming,
    sessionModes,
  } = useChatStore()

  const openFile = useUIStore((s) => s.openFile)
  const flowgroupEditor = useUIStore((s) => s.flowgroupEditor)
  const isCreatingSessionRef = useRef(false)

  const isStreaming = sessionStreaming[activeSessionId ?? ''] ?? false

  const sendUserMessage = useCallback(
    async (text: string) => {
      // Auto-create session if none active
      let sessionId = activeSessionId
      if (!sessionId) {
        if (isCreatingSessionRef.current) return
        isCreatingSessionRef.current = true
        try {
          const response = await createAISession(defaultMode)
          sessionId = response.session_id
          setActiveSession(sessionId)
          const newSession = {
            id: sessionId,
            title: text.slice(0, 50),
            createdAt: Date.now(),
            messageCount: 0,
            mode: defaultMode,
            unreadCount: 0,
          }
          addSession(newSession)
          // Track mode in store and add to available sessions
          useChatStore.setState((s) => ({
            sessionModes: { ...s.sessionModes, [sessionId!]: defaultMode },
            availableSessions: [newSession, ...s.availableSessions],
          }))
        } catch {
          return
        } finally {
          isCreatingSessionRef.current = false
        }
      }

      // Build context prefix from current UI state
      let contextPrefix = ''
      if (chatContext) {
        if (chatContext.type === 'file' && chatContext.path) {
          contextPrefix = `[Context: editing file ${chatContext.path}]\n\n`
        } else if (chatContext.type === 'flowgroup') {
          contextPrefix = `[Context: viewing flowgroup "${chatContext.name}"]\n\n`
        }
      } else if (openFile) {
        contextPrefix = `[Context: editing file ${openFile.path}]\n\n`
      } else if (flowgroupEditor) {
        contextPrefix = `[Context: editing flowgroup "${flowgroupEditor.name}" in pipeline "${flowgroupEditor.pipeline}"]\n\n`
      }

      // Optimistic user message
      const userMsg: ChatMessage = {
        id: nextMessageId(),
        role: 'user',
        parts: [{ type: 'text', text }],
        timestamp: Date.now(),
      }
      addMessageToSession(sessionId, userMsg)
      setSessionStreaming(sessionId, true)

      // Generate LLM title on first message (fire-and-forget)
      const existingMessages = useChatStore.getState().sessionMessages[sessionId] ?? []
      if (existingMessages.length <= 1) {
        generateSessionTitle(sessionId, [{ type: 'text', text }]).catch(() => {})
      }

      try {
        // Fire-and-forget: POST the message, SSE stream delivers the response
        await sendAIMessage(sessionId, [{ type: 'text', text: contextPrefix + text }])
      } catch (err) {
        const errorText = err instanceof Error ? err.message : 'Unknown error'
        const isTimeout = errorText.includes('504') || errorText.includes('Gateway Timeout')

        if (isTimeout) {
          return
        }

        addMessageToSession(sessionId, {
          id: nextMessageId(),
          role: 'assistant',
          parts: [{ type: 'text', text: `Error: ${errorText}` }],
          timestamp: Date.now(),
          isStreaming: false,
        })
        setSessionStreaming(sessionId, false)
        // Finalize all in-flight messages so "Thinking..." clears
        const state = useChatStore.getState()
        const msgs = state.sessionMessages[sessionId] ?? []
        for (const msg of msgs) {
          if (msg.isStreaming) state.finalizeMessage(sessionId, msg.id)
        }
      }
    },
    [activeSessionId, setActiveSession, addSession, addMessageToSession, setSessionStreaming, chatContext, openFile, flowgroupEditor, defaultMode, sessionModes],
  )

  const stopGeneration = useCallback(async () => {
    if (!activeSessionId) return
    try {
      await cancelAIGeneration(activeSessionId)
    } catch {
      // Best-effort cancellation
    }
    setSessionStreaming(activeSessionId, false)
    // Finalize all in-flight messages so per-message isStreaming clears
    const state = useChatStore.getState()
    const msgs = state.sessionMessages[activeSessionId] ?? []
    for (const msg of msgs) {
      if (msg.isStreaming) {
        state.finalizeMessage(activeSessionId, msg.id)
      }
    }
  }, [activeSessionId, setSessionStreaming])

  return { sendMessage: sendUserMessage, stopGeneration, isStreaming }
}
