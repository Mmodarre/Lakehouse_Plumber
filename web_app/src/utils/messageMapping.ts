/**
 * Shared mapping utilities for converting raw OpenCode messages
 * to the ChatMessage format used by the store.
 *
 * Used by both ChatPanel (session restore) and useSSEStream
 * (reconnect catch-up).
 */

import type { ChatMessage, ChatMessagePart, SSEToolState, ToolState } from '../types/chat'

/**
 * Map raw OpenCode message parts to ChatMessagePart[].
 */
export function mapRestoredParts(msg: Record<string, unknown>): ChatMessagePart[] {
  if (Array.isArray(msg.parts)) {
    const result: ChatMessagePart[] = []
    for (const p of msg.parts as Array<Record<string, unknown>>) {
      const type = p.type as string
      if (type === 'text') {
        result.push({ type: 'text', text: p.text as string })
      } else if (type === 'reasoning') {
        result.push({ type: 'reasoning', text: p.text as string })
      } else if (type === 'tool') {
        // OpenCode ToolPart: { tool, state: { status, input, output, error, title } }
        const toolName = (p.tool ?? p.name) as string | undefined
        const stateObj = p.state as SSEToolState | undefined
        const status: ToolState = stateObj?.status ?? 'completed'
        const input = stateObj?.input
        const title = stateObj?.title

        const toolArgs: Record<string, unknown> = { ...input }
        if (title) toolArgs._title = title

        const part: ChatMessagePart = {
          type: 'tool-call',
          toolName: toolName ?? undefined,
          toolArgs: Object.keys(toolArgs).length > 0 ? toolArgs : undefined,
          toolState: status,
        }
        if (status === 'completed' && stateObj?.output !== undefined) {
          part.toolResult = typeof stateObj.output === 'string'
            ? stateObj.output
            : JSON.stringify(stateObj.output)
        }
        if (status === 'error') {
          part.isError = true
          part.toolError = (stateObj?.error as string) ?? undefined
        }
        result.push(part)
      }
    }
    return result
  }

  // Fallback: user messages with text content
  if (msg.role === 'user') {
    return [{ type: 'text', text: String(msg.text ?? msg.content ?? '') }]
  }

  return []
}

/**
 * Map a single raw OpenCode message to a ChatMessage.
 */
export function mapRawMessage(msg: Record<string, unknown>): ChatMessage {
  return {
    id: (msg.id as string) ?? `restored-${Date.now()}-${Math.random()}`,
    role: (msg.role as 'user' | 'assistant') ?? 'assistant',
    parts: mapRestoredParts(msg),
    timestamp: ((msg.time as Record<string, unknown>)?.created as number) ?? Date.now(),
    isStreaming: false,
  }
}
