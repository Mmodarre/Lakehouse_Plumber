/** Chat panel types for OpenCode AI assistant integration. */

export type ConnectionStatus = 'disconnected' | 'connecting' | 'connected' | 'reconnecting'

export type ChatContext = {
  type: 'file' | 'flowgroup' | 'page'
  name: string
  path?: string
} | null

export interface ChatMessagePart {
  type: 'text' | 'reasoning' | 'tool-call' | 'tool-result'
  text?: string
  toolName?: string
  toolArgs?: Record<string, unknown>
  toolResult?: string
  isError?: boolean
}

export interface ChatMessage {
  id: string
  role: 'user' | 'assistant'
  parts: ChatMessagePart[]
  timestamp: number
  isStreaming?: boolean
}

export interface SessionSummary {
  id: string
  title: string
  createdAt: number
  messageCount: number
}

export interface AIStatus {
  available: boolean
  url: string | null
  auth_required: boolean
}
