/**
 * Core chat hook — send/stop message orchestration.
 *
 * Manages the lifecycle of sending a message: optimistic UI update,
 * OpenCode API call, response parsing, and final state cleanup.
 *
 * OpenCode v1.2.10 uses synchronous JSON responses (not SSE streaming).
 * The `sendMessage` call blocks until the LLM finishes, then returns
 * the full response with a `parts` array.
 */

import { useCallback } from 'react'
import { useChatStore } from '../store/chatStore'
import { useUIStore } from '../store/uiStore'
import {
  getOpenCodeClient,
  sendMessage,
  cancelGeneration,
  createOpenCodeSession,
} from '../api/opencode'
import { createAISession } from '../api/ai'
import type { OpenCodePart } from '../api/opencode'
import type { ChatMessage, ChatMessagePart } from '../types/chat'

let _messageCounter = 0
function nextMessageId(): string {
  return `msg-${Date.now()}-${++_messageCounter}`
}

export function useChat() {
  const {
    activeSessionId,
    setActiveSession,
    addSession,
    addMessage,
    updateMessage,
    setStreaming,
    chatContext,
    isStreaming,
  } = useChatStore()

  const openFile = useUIStore((s) => s.openFile)
  const flowgroupEditor = useUIStore((s) => s.flowgroupEditor)

  const sendUserMessage = useCallback(
    async (text: string) => {
      const client = getOpenCodeClient()
      if (!client) return

      // Auto-create session if none active
      let sessionId = activeSessionId
      if (!sessionId) {
        try {
          // Step 1: Tell the backend to ensure OpenCode is running in the
          // correct workspace directory (production mode: restarts subprocess
          // with cwd=workspace_root; dev mode: no-op).
          await createAISession()

          // Step 2: Create the actual OpenCode session (now scoped to the
          // correct workspace because the backend restarted OpenCode there).
          const session = await createOpenCodeSession(client)
          sessionId = session.id
          setActiveSession(sessionId)
          addSession({
            id: sessionId,
            title: text.slice(0, 50),
            createdAt: Date.now(),
            messageCount: 0,
          })
        } catch {
          return
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
      addMessage(userMsg)

      // Create placeholder assistant message (shown as "thinking...")
      const assistantMsgId = nextMessageId()
      const assistantMsg: ChatMessage = {
        id: assistantMsgId,
        role: 'assistant',
        parts: [],
        timestamp: Date.now(),
        isStreaming: true,
      }
      addMessage(assistantMsg)
      setStreaming(true)

      try {
        // OpenCode returns synchronous JSON — this blocks until the LLM finishes
        const response = await sendMessage(
          client,
          sessionId,
          contextPrefix + text,
        )

        // Map OpenCode parts → ChatMessageParts
        const chatParts = mapOpenCodeParts(response.parts)

        updateMessage(assistantMsgId, (msg) => ({
          ...msg,
          parts: chatParts,
          isStreaming: false,
        }))
      } catch (err) {
        const errorText = err instanceof Error ? err.message : 'Unknown error'
        updateMessage(assistantMsgId, (msg) => ({
          ...msg,
          parts: [{ type: 'text', text: `Error: ${errorText}` }],
          isStreaming: false,
        }))
      } finally {
        setStreaming(false)
      }
    },
    [activeSessionId, setActiveSession, addSession, addMessage, updateMessage, setStreaming, chatContext, openFile, flowgroupEditor],
  )

  const stopGeneration = useCallback(async () => {
    const client = getOpenCodeClient()
    if (!client || !activeSessionId) return
    try {
      await cancelGeneration(client, activeSessionId)
    } catch {
      // Best-effort cancellation
    }
    setStreaming(false)
  }, [activeSessionId, setStreaming])

  return { sendMessage: sendUserMessage, stopGeneration, isStreaming }
}

/**
 * Map OpenCode response parts to our ChatMessagePart format.
 *
 * OpenCode parts include metadata types (step-start, step-finish) that
 * we filter out, keeping only user-visible content:
 * - text     → displayed as markdown
 * - reasoning → displayed as collapsible "thinking" block
 * - tool-call / tool-result → displayed as tool cards
 */
function mapOpenCodeParts(parts: OpenCodePart[]): ChatMessagePart[] {
  const chatParts: ChatMessagePart[] = []

  for (const part of parts) {
    switch (part.type) {
      case 'text':
        if (part.text) {
          // Merge consecutive text parts
          const lastPart = chatParts[chatParts.length - 1]
          if (lastPart?.type === 'text') {
            lastPart.text = (lastPart.text ?? '') + part.text
          } else {
            chatParts.push({ type: 'text', text: part.text })
          }
        }
        break

      case 'reasoning':
        if (part.text) {
          chatParts.push({ type: 'reasoning', text: part.text })
        }
        break

      case 'tool-call':
        chatParts.push({
          type: 'tool-call',
          toolName: part.name,
          toolArgs: part.args as Record<string, unknown>,
        })
        break

      case 'tool-result':
        chatParts.push({
          type: 'tool-result',
          toolName: part.name,
          toolResult: typeof part.result === 'string' ? part.result : JSON.stringify(part.result),
          isError: part.isError,
        })
        break

      // Skip metadata types: step-start, step-finish
      default:
        break
    }
  }

  return chatParts
}
