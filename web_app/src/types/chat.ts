/** Chat panel types for OpenCode AI assistant integration. */

export type ConnectionStatus = 'disconnected' | 'connecting' | 'connected' | 'reconnecting'

/** Session mode: 'agent' has full access, 'chat' is read-only (no file edits). */
export type SessionMode = 'agent' | 'chat'

export type ChatContext = {
  type: 'file' | 'flowgroup' | 'page'
  name: string
  path?: string
} | null

export type ToolState = 'pending' | 'running' | 'completed' | 'error'

/** Wire format for tool state in OpenCode SSE events. */
export interface SSEToolState {
  status: ToolState
  input?: Record<string, unknown>
  output?: string
  error?: string
  title?: string
}

export interface ChatMessagePart {
  type: 'text' | 'reasoning' | 'tool-call'
  text?: string
  toolName?: string
  toolArgs?: Record<string, unknown>
  toolResult?: string
  toolState?: ToolState
  toolError?: string
  isError?: boolean
  _partId?: string
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
  mode: SessionMode
  unreadCount: number
}

export interface AIStatus {
  available: boolean
  url: string | null
  auth_required: boolean
  config?: {
    provider: string
    model: string
    allowed_models: Record<string, string[]>
  } | null
}

// ── SSE event types (from @opencode-ai/sdk) ─────────────────
// We re-export the types we care about for the SSE stream handler.
// The backend proxies OpenCode's /event endpoint as-is.

export interface SSEEvent {
  type: string
  properties: Record<string, unknown>
}

/** message.updated — marks a message as complete (or in-progress). */
export interface SSEMessageUpdated extends SSEEvent {
  type: 'message.updated'
  properties: {
    info: {
      id: string
      sessionID: string
      role: 'user' | 'assistant'
      time: { created: number; completed?: number }
      parentID?: string
      modelID?: string
      [key: string]: unknown
    }
  }
}

/** message.part.updated — a part was created or updated (includes delta). */
export interface SSEMessagePartUpdated extends SSEEvent {
  type: 'message.part.updated'
  properties: {
    part: {
      id: string
      sessionID: string
      messageID: string
      type: string // 'text', 'reasoning', 'tool', 'step-start', 'step-finish', etc.
      text?: string
      tool?: string // tool name (e.g. "Read", "Bash")
      state?: SSEToolState // structured tool state
      [key: string]: unknown
    }
    delta?: string
  }
}

/** message.removed — a message was deleted. */
export interface SSEMessageRemoved extends SSEEvent {
  type: 'message.removed'
  properties: {
    sessionID: string
    messageID: string
  }
}

/** session.idle — the session finished processing. */
export interface SSESessionIdle extends SSEEvent {
  type: 'session.idle'
  properties: {
    sessionID: string
  }
}

/** session.error — provider auth failure or output limit error. */
export interface SSESessionError extends SSEEvent {
  type: 'session.error'
  properties: {
    sessionID: string
    error: string
  }
}

/** session.updated — session metadata changed. */
export interface SSESessionUpdated extends SSEEvent {
  type: 'session.updated'
  properties: {
    info: {
      id: string
      title: string
      [key: string]: unknown
    }
  }
}

/** question.asked — the LLM asked the user a question via the question tool. */
export interface SSEQuestionAsked extends SSEEvent {
  type: 'question.asked'
  properties: {
    id: string // question request ID (for reply/reject)
    sessionID: string
    questions: Array<{
      question: string
      header?: string
      options: Array<{ label: string; description?: string }>
      multiple?: boolean
      custom?: boolean
    }>
    tool: {
      messageID: string
      callID: string // matches the tool part's _partId
    }
  }
}
