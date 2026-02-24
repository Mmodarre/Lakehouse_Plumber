/**
 * Chat panel state management — separate Zustand store.
 *
 * Kept separate from uiStore to avoid coupling AI-specific state
 * with the core UI state. Uses the same flat state + setter pattern.
 */

import { create } from 'zustand'
import type { ChatContext, ChatMessage, ConnectionStatus, SessionSummary } from '../types/chat'

interface ChatState {
  // Panel visibility
  chatPanelOpen: boolean
  toggleChatPanel: () => void
  setChatPanelOpen: (open: boolean) => void

  // Connection
  connectionStatus: ConnectionStatus
  setConnectionStatus: (status: ConnectionStatus) => void

  // AI availability (from /api/ai/status)
  aiAvailable: boolean
  openCodeUrl: string | null
  setAIStatus: (available: boolean, url: string | null) => void

  // Sessions
  sessions: SessionSummary[]
  activeSessionId: string | null
  setSessions: (sessions: SessionSummary[]) => void
  setActiveSession: (id: string | null) => void
  addSession: (session: SessionSummary) => void
  removeSession: (id: string) => void

  // Messages
  messages: ChatMessage[]
  setMessages: (messages: ChatMessage[]) => void
  addMessage: (message: ChatMessage) => void
  updateMessage: (id: string, updater: (msg: ChatMessage) => ChatMessage) => void

  // Streaming
  isStreaming: boolean
  setStreaming: (streaming: boolean) => void

  // Context (auto-detected from uiStore)
  chatContext: ChatContext
  setChatContext: (context: ChatContext) => void

  // Provider config (persisted to localStorage separately)
  selectedProvider: string
  selectedModel: string
  setProvider: (provider: string, model: string) => void
}

export const useChatStore = create<ChatState>((set) => ({
  // Panel visibility
  chatPanelOpen: false,
  toggleChatPanel: () => set((s) => ({ chatPanelOpen: !s.chatPanelOpen })),
  setChatPanelOpen: (open) => set({ chatPanelOpen: open }),

  // Connection
  connectionStatus: 'disconnected',
  setConnectionStatus: (status) => set({ connectionStatus: status }),

  // AI availability
  aiAvailable: false,
  openCodeUrl: null,
  setAIStatus: (available, url) => set({ aiAvailable: available, openCodeUrl: url }),

  // Sessions
  sessions: [],
  activeSessionId: null,
  setSessions: (sessions) => set({ sessions }),
  setActiveSession: (id) => set({ activeSessionId: id, messages: [] }),
  addSession: (session) => set((s) => ({ sessions: [...s.sessions, session] })),
  removeSession: (id) =>
    set((s) => ({
      sessions: s.sessions.filter((sess) => sess.id !== id),
      activeSessionId: s.activeSessionId === id ? null : s.activeSessionId,
      messages: s.activeSessionId === id ? [] : s.messages,
    })),

  // Messages
  messages: [],
  setMessages: (messages) => set({ messages }),
  addMessage: (message) => set((s) => ({ messages: [...s.messages, message] })),
  updateMessage: (id, updater) =>
    set((s) => ({
      messages: s.messages.map((m) => (m.id === id ? updater(m) : m)),
    })),

  // Streaming
  isStreaming: false,
  setStreaming: (streaming) => set({ isStreaming: streaming }),

  // Context
  chatContext: null,
  setChatContext: (context) => set({ chatContext: context }),

  // Provider config
  selectedProvider: localStorage.getItem('lhp-ai-provider') ?? 'anthropic',
  selectedModel: localStorage.getItem('lhp-ai-model') ?? 'claude-sonnet-4-20250514',
  setProvider: (provider, model) => {
    localStorage.setItem('lhp-ai-provider', provider)
    localStorage.setItem('lhp-ai-model', model)
    set({ selectedProvider: provider, selectedModel: model })
  },
}))
