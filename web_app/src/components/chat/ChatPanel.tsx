/**
 * Chat panel — right sidebar container for the AI assistant.
 *
 * Renders as a collapsible panel on the right side of the layout.
 * Handles keyboard shortcuts (Cmd+Shift+L toggle, Escape close, Cmd+. stop).
 *
 * Welcome screen: when no sessions are open, shows the WelcomeScreen
 * with new-chat buttons and a dropdown for previous sessions.
 *
 * Multi-session: the SSE stream is user-scoped (single connection for
 * all sessions). Messages are stored per-session in the store.
 */

import { useCallback, useEffect, useMemo, useState } from 'react'
import { ChatPanelHeader } from './ChatPanelHeader'
import { SessionTabBar } from './SessionTabBar'
import { ChatMessageList } from './ChatMessageList'
import { ChatInput } from './ChatInput'
import { ChatSettingsModal } from './ChatSettingsModal'
import { ConnectionStatusBanner } from './ConnectionStatus'
import { WelcomeScreen } from './WelcomeScreen'
import { ResizeHandle } from '../common/ResizeHandle'
import { useChatStore } from '../../store/chatStore'
import { useChat } from '../../hooks/useChat'
import { useSSEStream } from '../../hooks/useSSEStream'
import { createAISession, getSessionMessages } from '../../api/ai'
import { mapRawMessage } from '../../utils/messageMapping'
import type { ChatMessage, SessionMode, SessionSummary } from '../../types/chat'

export function ChatPanel() {
  const {
    chatPanelOpen,
    setChatPanelOpen,
    chatPanelWidth,
    setChatPanelWidth,
    connectionStatus,
    setConnectionStatus,
    aiAvailable,
    sessions,
    activeSessionId,
    setActiveSession,
    addSession,
    sessionMessages,
    chatContext,
    streamingStalled,
    availableSessions,
    fetchAvailableSessions,
    openSession,
  } = useChatStore()

  const { sendMessage, stopGeneration, isStreaming } = useChat()
  const [settingsOpen, setSettingsOpen] = useState(false)
  const [isLoadingHistory, setIsLoadingHistory] = useState(false)

  // Messages for the active session
  const messages = sessionMessages[activeSessionId ?? ''] ?? []

  // Set of open session IDs (for filtering in header dropdown)
  const openSessionIds = useMemo(() => new Set(sessions.map((s) => s.id)), [sessions])

  // User-scoped SSE stream — single connection for all sessions
  useSSEStream(aiAvailable && connectionStatus !== 'disconnected')

  // When AI becomes available: fetch available sessions + set connected
  useEffect(() => {
    if (!aiAvailable) {
      setConnectionStatus('disconnected')
      return
    }

    setConnectionStatus('connecting')

    const init = async () => {
      setIsLoadingHistory(true)
      try {
        await fetchAvailableSessions()
      } finally {
        setIsLoadingHistory(false)
      }
      setConnectionStatus('connected')
    }

    init()
  }, [aiAvailable]) // eslint-disable-line react-hooks/exhaustive-deps

  // When switching tabs, load messages if not already in store
  useEffect(() => {
    if (!activeSessionId) return
    const existing = sessionMessages[activeSessionId]
    if (!existing || existing.length === 0) {
      loadSessionMessages(activeSessionId)
    }
  }, [activeSessionId]) // eslint-disable-line react-hooks/exhaustive-deps

  // Global keyboard shortcuts
  useEffect(() => {
    function handleKeyDown(e: KeyboardEvent) {
      if ((e.metaKey || e.ctrlKey) && e.shiftKey && e.key === 'l') {
        e.preventDefault()
        setChatPanelOpen(!chatPanelOpen)
      }
      if (e.key === 'Escape' && chatPanelOpen) {
        setChatPanelOpen(false)
      }
      if ((e.metaKey || e.ctrlKey) && e.key === '.' && isStreaming) {
        e.preventDefault()
        stopGeneration()
      }
    }

    window.addEventListener('keydown', handleKeyDown)
    return () => window.removeEventListener('keydown', handleKeyDown)
  }, [chatPanelOpen, setChatPanelOpen, isStreaming, stopGeneration])

  const handleSuggestion = useCallback(
    (text: string) => {
      sendMessage(text)
    },
    [sendMessage],
  )

  const handleResize = useCallback(
    (delta: number) => {
      const current = useChatStore.getState().chatPanelWidth
      setChatPanelWidth(current + delta)
    },
    [setChatPanelWidth],
  )

  const handleResetWidth = useCallback(
    () => setChatPanelWidth(384),
    [setChatPanelWidth],
  )

  const handleNewChat = useCallback(async (mode: SessionMode) => {
    try {
      const response = await createAISession(mode)
      const sessionId = response.session_id
      const newSession: SessionSummary = {
        id: sessionId,
        title: 'New Chat',
        createdAt: Date.now(),
        messageCount: 0,
        mode,
        unreadCount: 0,
      }
      addSession(newSession)
      // Track mode in store, update defaultMode, and add to available sessions
      useChatStore.setState((s) => ({
        sessionModes: { ...s.sessionModes, [sessionId]: mode },
        defaultMode: mode,
        availableSessions: [newSession, ...s.availableSessions],
      }))
      setActiveSession(sessionId)
    } catch {
      // Session creation failed — ignore
    }
  }, [addSession, setActiveSession])

  const handleOpenSession = useCallback((session: SessionSummary) => {
    openSession(session)
  }, [openSession])

  if (!chatPanelOpen) return null

  const hasOpenSessions = sessions.length > 0

  return (
    <>
      <aside
        className="relative flex shrink-0 flex-col border-l border-slate-200 bg-slate-50"
        style={{ width: chatPanelWidth }}
      >
        <ResizeHandle onResize={handleResize} onReset={handleResetWidth} />

        <ChatPanelHeader
          connectionStatus={connectionStatus}
          onClose={() => setChatPanelOpen(false)}
          onSettings={() => setSettingsOpen(true)}
          onNewChat={handleNewChat}
          availableSessions={availableSessions}
          onOpenSession={handleOpenSession}
          openSessionIds={openSessionIds}
        />

        {hasOpenSessions ? (
          <>
            <SessionTabBar />

            <ConnectionStatusBanner status={connectionStatus} streamingStalled={streamingStalled} />

            <ChatMessageList
              messages={messages}
              onSuggestion={handleSuggestion}
            />

            <ChatInput
              onSend={sendMessage}
              onStop={stopGeneration}
              isStreaming={isStreaming}
              context={chatContext}
              disabled={connectionStatus !== 'connected'}
            />
          </>
        ) : (
          <WelcomeScreen
            onNewChat={handleNewChat}
            availableSessions={availableSessions}
            onOpenSession={handleOpenSession}
            isLoadingHistory={isLoadingHistory}
          />
        )}
      </aside>

      <ChatSettingsModal
        open={settingsOpen}
        onClose={() => setSettingsOpen(false)}
      />
    </>
  )
}

/**
 * Load messages from a persisted session and map them to chat format.
 */
async function loadSessionMessages(sessionId: string) {
  try {
    const rawMessages = await getSessionMessages(sessionId)
    const chatMessages: ChatMessage[] = (rawMessages as Array<Record<string, unknown>>).map(mapRawMessage)
    useChatStore.getState().setSessionMessages(sessionId, chatMessages)
  } catch {
    // Failed to load messages — start fresh
  }
}
