/**
 * Chat panel — right sidebar container for the AI assistant.
 *
 * Renders as a collapsible panel on the right side of the layout.
 * Handles keyboard shortcuts (Cmd+Shift+L toggle, Escape close, Cmd+. stop).
 */

import { useCallback, useEffect, useState } from 'react'
import { ChatPanelHeader } from './ChatPanelHeader'
import { ChatMessageList } from './ChatMessageList'
import { ChatInput } from './ChatInput'
import { ChatSettingsModal } from './ChatSettingsModal'
import { ConnectionStatusBanner } from './ConnectionStatus'
import { useChatStore } from '../../store/chatStore'
import { useChat } from '../../hooks/useChat'
import { initOpenCodeClient, resetOpenCodeClient } from '../../api/opencode'

export function ChatPanel() {
  const {
    chatPanelOpen,
    setChatPanelOpen,
    connectionStatus,
    setConnectionStatus,
    aiAvailable,
    openCodeUrl,
    messages,
    chatContext,
    isStreaming,
  } = useChatStore()

  const { sendMessage, stopGeneration } = useChat()
  const [settingsOpen, setSettingsOpen] = useState(false)

  // Initialize/teardown OpenCode client when AI becomes available.
  // In dev (Vite), use the /opencode proxy path so the browser doesn't
  // need CORS to reach the OpenCode server. In production, use the
  // actual URL from the backend (assumes a reverse proxy is in place).
  useEffect(() => {
    if (aiAvailable && openCodeUrl) {
      const proxyUrl = import.meta.env.DEV ? '/opencode' : openCodeUrl
      initOpenCodeClient(proxyUrl)
      setConnectionStatus('connected')
    } else {
      resetOpenCodeClient()
      setConnectionStatus('disconnected')
    }
  }, [aiAvailable, openCodeUrl, setConnectionStatus])

  // Global keyboard shortcuts
  useEffect(() => {
    function handleKeyDown(e: KeyboardEvent) {
      // Cmd/Ctrl + Shift + L → toggle chat panel
      if ((e.metaKey || e.ctrlKey) && e.shiftKey && e.key === 'l') {
        e.preventDefault()
        setChatPanelOpen(!chatPanelOpen)
      }
      // Escape → close chat panel
      if (e.key === 'Escape' && chatPanelOpen) {
        setChatPanelOpen(false)
      }
      // Cmd/Ctrl + . → stop generation
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

  if (!chatPanelOpen) return null

  return (
    <>
      <aside className="flex w-96 shrink-0 flex-col border-l border-slate-200 bg-slate-50">
        <ChatPanelHeader
          connectionStatus={connectionStatus}
          onClose={() => setChatPanelOpen(false)}
          onSettings={() => setSettingsOpen(true)}
        />

        <ConnectionStatusBanner status={connectionStatus} />

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
      </aside>

      <ChatSettingsModal
        open={settingsOpen}
        onClose={() => setSettingsOpen(false)}
      />
    </>
  )
}
