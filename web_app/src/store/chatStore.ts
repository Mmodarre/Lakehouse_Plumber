/**
 * Chat panel state management — separate Zustand store.
 *
 * Kept separate from uiStore to avoid coupling AI-specific state
 * with the core UI state. Uses the same flat state + setter pattern.
 *
 * Multi-session support: messages, streaming, and mode are tracked
 * per-session via Record<sessionId, ...> maps.  The `messages` getter
 * returns messages for the active session for backwards compat.
 *
 * SSE-driven update methods (appendMessageDelta, replaceMessagePart,
 * finalizeMessage) accept a sessionId parameter to target the correct
 * session's message slice.
 */

import { create } from 'zustand'
import { listAISessions } from '../api/ai'
import type { ChatContext, ChatMessage, ChatMessagePart, ConnectionStatus, SessionMode, SessionSummary } from '../types/chat'

interface ChatState {
  // Panel visibility
  chatPanelOpen: boolean
  toggleChatPanel: () => void
  setChatPanelOpen: (open: boolean) => void

  // Panel width (resizable)
  chatPanelWidth: number
  setChatPanelWidth: (w: number) => void

  // Connection
  connectionStatus: ConnectionStatus
  setConnectionStatus: (status: ConnectionStatus) => void

  // AI availability (from /api/ai/status)
  aiAvailable: boolean
  setAIStatus: (available: boolean) => void

  // Sessions (open tabs)
  sessions: SessionSummary[]
  activeSessionId: string | null
  setSessions: (sessions: SessionSummary[]) => void
  setActiveSession: (id: string | null) => void
  addSession: (session: SessionSummary) => void
  removeSession: (id: string) => void
  updateSessionTitle: (id: string, title: string) => void

  // Available sessions (backend history for dropdown/welcome screen)
  availableSessions: SessionSummary[]
  setAvailableSessions: (sessions: SessionSummary[]) => void
  fetchAvailableSessions: () => Promise<void>
  openSession: (session: SessionSummary) => void
  closeTab: (id: string) => void

  // Per-session messages
  sessionMessages: Record<string, ChatMessage[]>
  /** Convenience getter — returns messages for the active session. */
  messages: ChatMessage[]
  setMessages: (messages: ChatMessage[]) => void
  setSessionMessages: (sessionId: string, messages: ChatMessage[]) => void
  addMessage: (message: ChatMessage) => void
  addMessageToSession: (sessionId: string, message: ChatMessage) => void
  updateMessage: (id: string, updater: (msg: ChatMessage) => ChatMessage) => void

  // SSE-driven message updates (now session-aware)
  ensureAssistantMessage: (messageId: string, sessionId: string) => void
  appendMessageDelta: (sessionId: string, messageId: string, partId: string, partType: string, delta: string) => void
  replaceMessagePart: (sessionId: string, messageId: string, partId: string, part: ChatMessagePart) => void
  finalizeMessage: (sessionId: string, messageId: string) => void

  // Per-session streaming
  sessionStreaming: Record<string, boolean>
  /** Convenience getter — returns streaming state for the active session. */
  isStreaming: boolean
  setStreaming: (streaming: boolean) => void
  setSessionStreaming: (sessionId: string, streaming: boolean) => void
  streamingStalled: boolean
  setStreamingStalled: (stalled: boolean) => void

  // Per-session mode tracking
  sessionModes: Record<string, SessionMode>
  /** Default mode for next new session (persisted to localStorage). */
  defaultMode: SessionMode
  setDefaultMode: (mode: SessionMode) => void

  // Unread counts
  incrementUnread: (sessionId: string) => void
  clearUnread: (sessionId: string) => void

  // Context (auto-detected from uiStore)
  chatContext: ChatContext
  setChatContext: (context: ChatContext) => void

  // Question tool tracking (sessionId → questionRequestId)
  pendingQuestionIds: Record<string, string>
  setPendingQuestionId: (sessionId: string, requestId: string) => void
  removePendingQuestionId: (sessionId: string) => void

  // Provider config (persisted to localStorage separately)
  selectedProvider: string
  selectedModel: string
  setProvider: (provider: string, model: string) => void
}

export const useChatStore = create<ChatState>((set, get) => ({
  // Panel visibility
  chatPanelOpen: false,
  toggleChatPanel: () => set((s) => ({ chatPanelOpen: !s.chatPanelOpen })),
  setChatPanelOpen: (open) => set({ chatPanelOpen: open }),

  // Panel width (resizable, persisted to localStorage)
  chatPanelWidth: (() => {
    const stored = localStorage.getItem('lhp-chat-panel-width')
    if (stored) {
      const parsed = parseInt(stored, 10)
      if (!isNaN(parsed) && parsed >= 320 && parsed <= 700) return parsed
    }
    return 384
  })(),
  setChatPanelWidth: (w) => {
    const clamped = Math.min(700, Math.max(320, w))
    localStorage.setItem('lhp-chat-panel-width', String(clamped))
    set({ chatPanelWidth: clamped })
  },

  // Connection
  connectionStatus: 'disconnected',
  setConnectionStatus: (status) => set({ connectionStatus: status }),

  // AI availability
  aiAvailable: false,
  setAIStatus: (available) => set({ aiAvailable: available }),

  // Sessions
  sessions: [],
  activeSessionId: null,
  setSessions: (sessions) => set({ sessions }),
  setActiveSession: (id) => {
    // Don't clear messages — they're per-session now.  Just clear unread.
    set((s) => {
      const updated = s.sessions.map((sess) =>
        sess.id === id ? { ...sess, unreadCount: 0 } : sess,
      )
      return { activeSessionId: id, sessions: updated }
    })
  },
  addSession: (session) => set((s) => ({ sessions: [...s.sessions, session] })),
  removeSession: (id) =>
    set((s) => {
      const remaining = s.sessions.filter((sess) => sess.id !== id)
      // Pick next tab: prefer the tab to the left, then right, then null
      let nextActive = s.activeSessionId
      if (s.activeSessionId === id) {
        const oldIdx = s.sessions.findIndex((sess) => sess.id === id)
        if (remaining.length > 0) {
          nextActive = remaining[Math.min(oldIdx, remaining.length - 1)].id
        } else {
          nextActive = null
        }
      }
      // Clean up per-session state
      const { [id]: _msgs, ...restMessages } = s.sessionMessages
      const { [id]: _str, ...restStreaming } = s.sessionStreaming
      const { [id]: _mode, ...restModes } = s.sessionModes
      return {
        sessions: remaining,
        activeSessionId: nextActive,
        sessionMessages: restMessages,
        sessionStreaming: restStreaming,
        sessionModes: restModes,
      }
    }),
  updateSessionTitle: (id, title) =>
    set((s) => ({
      sessions: s.sessions.map((sess) =>
        sess.id === id ? { ...sess, title } : sess,
      ),
      availableSessions: s.availableSessions.map((sess) =>
        sess.id === id ? { ...sess, title } : sess,
      ),
    })),

  // Available sessions (backend history)
  availableSessions: [],
  setAvailableSessions: (sessions) => set({ availableSessions: sessions }),
  fetchAvailableSessions: async () => {
    try {
      const sessions = await listAISessions()
      const summaries = (sessions as Array<{ id: string; title?: string; time?: { created: number } }>).map((s) => ({
        id: s.id,
        title: s.title ?? 'Untitled',
        createdAt: s.time?.created ?? Date.now(),
        messageCount: 0,
        mode: 'agent' as const,
        unreadCount: 0,
      }))
      set({ availableSessions: summaries })
    } catch {
      // Failed to fetch — leave empty
    }
  },
  openSession: (session) =>
    set((s) => {
      // If already open as a tab, just switch to it
      if (s.sessions.some((sess) => sess.id === session.id)) {
        return { activeSessionId: session.id }
      }
      // Add as new tab and switch to it
      return {
        sessions: [...s.sessions, { ...session, unreadCount: 0 }],
        activeSessionId: session.id,
      }
    }),
  closeTab: (id) =>
    set((s) => {
      const remaining = s.sessions.filter((sess) => sess.id !== id)
      // Pick next tab: prefer the tab to the left, then right, then null
      let nextActive = s.activeSessionId
      if (s.activeSessionId === id) {
        const oldIdx = s.sessions.findIndex((sess) => sess.id === id)
        if (remaining.length > 0) {
          nextActive = remaining[Math.min(oldIdx, remaining.length - 1)].id
        } else {
          nextActive = null
        }
      }
      // Clean up per-session state
      const { [id]: _msgs, ...restMessages } = s.sessionMessages
      const { [id]: _str, ...restStreaming } = s.sessionStreaming
      const { [id]: _mode, ...restModes } = s.sessionModes
      return {
        sessions: remaining,
        activeSessionId: nextActive,
        sessionMessages: restMessages,
        sessionStreaming: restStreaming,
        sessionModes: restModes,
      }
    }),

  // Per-session messages
  sessionMessages: {},
  get messages() {
    const state = get()
    return state.sessionMessages[state.activeSessionId ?? ''] ?? []
  },
  setMessages: (messages) =>
    set((s) => {
      if (!s.activeSessionId) return s
      return {
        sessionMessages: { ...s.sessionMessages, [s.activeSessionId]: messages },
      }
    }),
  setSessionMessages: (sessionId, messages) =>
    set((s) => ({
      sessionMessages: { ...s.sessionMessages, [sessionId]: messages },
    })),
  addMessage: (message) =>
    set((s) => {
      if (!s.activeSessionId) return s
      const existing = s.sessionMessages[s.activeSessionId] ?? []
      return {
        sessionMessages: {
          ...s.sessionMessages,
          [s.activeSessionId]: [...existing, message],
        },
      }
    }),
  addMessageToSession: (sessionId, message) =>
    set((s) => {
      const existing = s.sessionMessages[sessionId] ?? []
      return {
        sessionMessages: {
          ...s.sessionMessages,
          [sessionId]: [...existing, message],
        },
      }
    }),
  updateMessage: (id, updater) =>
    set((s) => {
      if (!s.activeSessionId) return s
      const msgs = s.sessionMessages[s.activeSessionId] ?? []
      return {
        sessionMessages: {
          ...s.sessionMessages,
          [s.activeSessionId]: msgs.map((m) => (m.id === id ? updater(m) : m)),
        },
      }
    }),

  // SSE-driven message updates (session-aware)
  ensureAssistantMessage: (messageId, sessionId) =>
    set((s) => {
      const msgs = s.sessionMessages[sessionId] ?? []
      if (msgs.some((m) => m.id === messageId)) return s
      return {
        sessionMessages: {
          ...s.sessionMessages,
          [sessionId]: [
            ...msgs,
            {
              id: messageId,
              role: 'assistant' as const,
              parts: [],
              timestamp: Date.now(),
              isStreaming: true,
            },
          ],
        },
      }
    }),

  appendMessageDelta: (sessionId, messageId, partId, partType, delta) =>
    set((s) => {
      const msgs = s.sessionMessages[sessionId] ?? []
      return {
        sessionMessages: {
          ...s.sessionMessages,
          [sessionId]: msgs.map((m) => {
            if (m.id !== messageId) return m
            const existing = m.parts.find((p) => p._partId === partId)
            if (existing && existing.type === 'text') {
              return {
                ...m,
                parts: m.parts.map((p) =>
                  p._partId === partId
                    ? { ...p, text: (p.text ?? '') + delta }
                    : p,
                ),
              }
            }
            const chatType = partType === 'text' ? 'text' : partType === 'reasoning' ? 'reasoning' : 'text'
            return {
              ...m,
              parts: [
                ...m.parts,
                { type: chatType, text: delta, _partId: partId } as ChatMessagePart & { _partId: string },
              ],
            }
          }),
        },
      }
    }),

  replaceMessagePart: (sessionId, messageId, partId, part) =>
    set((s) => {
      const msgs = s.sessionMessages[sessionId] ?? []
      return {
        sessionMessages: {
          ...s.sessionMessages,
          [sessionId]: msgs.map((m) => {
            if (m.id !== messageId) return m
            const idx = m.parts.findIndex((p) => p._partId === partId)
            if (idx >= 0) {
              const newParts = [...m.parts]
              newParts[idx] = { ...part, _partId: partId }
              return { ...m, parts: newParts }
            }
            return {
              ...m,
              parts: [...m.parts, { ...part, _partId: partId }],
            }
          }),
        },
      }
    }),

  finalizeMessage: (sessionId, messageId) =>
    set((s) => {
      const msgs = s.sessionMessages[sessionId] ?? []
      return {
        sessionMessages: {
          ...s.sessionMessages,
          [sessionId]: msgs.map((m) =>
            m.id === messageId ? { ...m, isStreaming: false } : m,
          ),
        },
      }
    }),

  // Per-session streaming
  sessionStreaming: {},
  get isStreaming() {
    const state = get()
    return state.sessionStreaming[state.activeSessionId ?? ''] ?? false
  },
  setStreaming: (streaming) =>
    set((s) => {
      if (!s.activeSessionId) return s
      return {
        sessionStreaming: { ...s.sessionStreaming, [s.activeSessionId]: streaming },
      }
    }),
  setSessionStreaming: (sessionId, streaming) =>
    set((s) => ({
      sessionStreaming: { ...s.sessionStreaming, [sessionId]: streaming },
    })),
  streamingStalled: false,
  setStreamingStalled: (stalled) => set({ streamingStalled: stalled }),

  // Per-session mode tracking
  sessionModes: {},
  defaultMode: (localStorage.getItem('lhp-chat-default-mode') as SessionMode) ?? 'agent',
  setDefaultMode: (mode) => {
    localStorage.setItem('lhp-chat-default-mode', mode)
    set({ defaultMode: mode })
  },

  // Unread counts
  incrementUnread: (sessionId) =>
    set((s) => {
      // Only increment if NOT the active session
      if (sessionId === s.activeSessionId) return s
      return {
        sessions: s.sessions.map((sess) =>
          sess.id === sessionId
            ? { ...sess, unreadCount: sess.unreadCount + 1 }
            : sess,
        ),
      }
    }),
  clearUnread: (sessionId) =>
    set((s) => ({
      sessions: s.sessions.map((sess) =>
        sess.id === sessionId ? { ...sess, unreadCount: 0 } : sess,
      ),
    })),

  // Context
  chatContext: null,
  setChatContext: (context) => set({ chatContext: context }),

  // Question tool tracking
  pendingQuestionIds: {},
  setPendingQuestionId: (partId, requestId) =>
    set((s) => ({ pendingQuestionIds: { ...s.pendingQuestionIds, [partId]: requestId } })),
  removePendingQuestionId: (partId) =>
    set((s) => {
      const { [partId]: _, ...rest } = s.pendingQuestionIds
      return { pendingQuestionIds: rest }
    }),

  // Provider config
  selectedProvider: localStorage.getItem('lhp-ai-provider') ?? 'anthropic',
  selectedModel: localStorage.getItem('lhp-ai-model') ?? 'anthropic/databricks-claude-sonnet-4-6',
  setProvider: (provider, model) => {
    localStorage.setItem('lhp-ai-provider', provider)
    localStorage.setItem('lhp-ai-model', model)
    set({ selectedProvider: provider, selectedModel: model })
  },
}))
