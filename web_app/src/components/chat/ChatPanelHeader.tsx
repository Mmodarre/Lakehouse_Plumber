/**
 * Chat panel header with session info, new-chat + history dropdown,
 * settings gear, and close button.
 *
 * The "+" dropdown combines mode selection (Agent/Chat) with a
 * scrollable list of previous sessions from backend history.
 */

import { useCallback, useEffect, useRef, useState } from 'react'
import { relativeTime } from '../../utils/timeFormat'
import type { ConnectionStatus, SessionMode, SessionSummary } from '../../types/chat'

const statusColors: Record<ConnectionStatus, string> = {
  disconnected: 'bg-slate-300',
  connecting: 'bg-blue-400 animate-pulse',
  connected: 'bg-green-500',
  reconnecting: 'bg-amber-400 animate-pulse',
}

export function ChatPanelHeader({
  connectionStatus,
  onClose,
  onSettings,
  onNewChat,
  availableSessions,
  onOpenSession,
  openSessionIds,
}: {
  connectionStatus: ConnectionStatus
  onClose: () => void
  onSettings: () => void
  onNewChat: (mode: SessionMode) => void
  availableSessions: SessionSummary[]
  onOpenSession: (session: SessionSummary) => void
  openSessionIds: Set<string>
}) {
  const [showMenu, setShowMenu] = useState(false)
  const menuRef = useRef<HTMLDivElement>(null)

  // Close dropdown when clicking outside
  useEffect(() => {
    if (!showMenu) return
    function handleClickOutside(e: MouseEvent) {
      if (menuRef.current && !menuRef.current.contains(e.target as Node)) {
        setShowMenu(false)
      }
    }
    document.addEventListener('mousedown', handleClickOutside)
    return () => document.removeEventListener('mousedown', handleClickOutside)
  }, [showMenu])

  const handleSelectMode = useCallback(
    (mode: SessionMode) => {
      setShowMenu(false)
      onNewChat(mode)
    },
    [onNewChat],
  )

  const handleSelectSession = useCallback(
    (session: SessionSummary) => {
      setShowMenu(false)
      onOpenSession(session)
    },
    [onOpenSession],
  )

  // Filter out sessions that are already open as tabs
  const resumableSessions = availableSessions.filter((s) => !openSessionIds.has(s.id))

  return (
    <div className="flex h-10 items-center border-b border-slate-200 bg-white px-3">
      {/* Status dot + title */}
      <div className="flex items-center gap-2">
        <div className={`h-1.5 w-1.5 rounded-full ${statusColors[connectionStatus]}`} />
        <span className="text-[11px] font-medium text-slate-700">AI Assistant</span>
      </div>

      <div className="ml-auto flex items-center gap-1">
        {/* New Chat / History dropdown */}
        <div className="relative" ref={menuRef}>
          <button
            onClick={() => setShowMenu((prev) => !prev)}
            className="rounded p-1 text-slate-400 hover:bg-slate-100 hover:text-slate-600"
            title="New chat"
          >
            <svg className="h-3.5 w-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
              <path strokeLinecap="round" strokeLinejoin="round" d="M12 4v16m8-8H4" />
            </svg>
          </button>

          {showMenu && (
            <div className="absolute right-0 top-full z-50 mt-1 w-52 rounded-md border border-slate-200 bg-white py-1 shadow-lg">
              {/* Mode selection */}
              <button
                onClick={() => handleSelectMode('agent')}
                className="flex w-full items-center gap-2 px-3 py-1.5 text-left text-[11px] hover:bg-slate-50"
              >
                <span className="flex h-4 w-4 items-center justify-center rounded text-[9px] font-bold text-white bg-blue-500">A</span>
                <div>
                  <div className="font-medium text-slate-700">Agent Chat</div>
                  <div className="text-[10px] text-slate-400">Full access</div>
                </div>
              </button>
              <button
                onClick={() => handleSelectMode('chat')}
                className="flex w-full items-center gap-2 px-3 py-1.5 text-left text-[11px] hover:bg-slate-50"
              >
                <span className="flex h-4 w-4 items-center justify-center rounded text-[9px] font-bold text-white bg-emerald-500">C</span>
                <div>
                  <div className="font-medium text-slate-700">Chat</div>
                  <div className="text-[10px] text-slate-400">Read-only</div>
                </div>
              </button>

              {/* Divider + Previous Sessions */}
              <div className="my-1 border-t border-slate-100" />
              <div className="px-3 py-1 text-[10px] font-medium text-slate-400">Previous Sessions</div>

              {resumableSessions.length === 0 ? (
                <div className="px-3 py-1.5 text-[11px] text-slate-400">
                  No previous sessions
                </div>
              ) : (
                <div className="max-h-[180px] overflow-y-auto">
                  {resumableSessions.map((session) => {
                    const mode = session.mode ?? 'agent'
                    const modeIcon = mode === 'chat' ? 'C' : 'A'
                    const modeBg = mode === 'chat' ? 'bg-emerald-500' : 'bg-blue-500'
                    return (
                      <button
                        key={session.id}
                        onClick={() => handleSelectSession(session)}
                        className="flex w-full items-center gap-2 px-3 py-1.5 text-left text-[11px] hover:bg-slate-50"
                      >
                        <span className={`flex h-4 w-4 shrink-0 items-center justify-center rounded text-[9px] font-bold text-white ${modeBg}`}>
                          {modeIcon}
                        </span>
                        <span className="min-w-0 flex-1 truncate text-slate-700">
                          {session.title || 'Untitled'}
                        </span>
                        <span className="shrink-0 text-[10px] text-slate-400">
                          {relativeTime(session.createdAt)}
                        </span>
                      </button>
                    )
                  })}
                </div>
              )}
            </div>
          )}
        </div>

        {/* Settings button */}
        <button
          onClick={onSettings}
          className="rounded p-1 text-slate-400 hover:bg-slate-100 hover:text-slate-600"
          title="AI settings"
        >
          <svg className="h-3.5 w-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
            <path strokeLinecap="round" strokeLinejoin="round" d="M10.325 4.317c.426-1.756 2.924-1.756 3.35 0a1.724 1.724 0 002.573 1.066c1.543-.94 3.31.826 2.37 2.37a1.724 1.724 0 001.066 2.573c1.756.426 1.756 2.924 0 3.35a1.724 1.724 0 00-1.066 2.573c.94 1.543-.826 3.31-2.37 2.37a1.724 1.724 0 00-2.573 1.066c-.426 1.756-2.924 1.756-3.35 0a1.724 1.724 0 00-2.573-1.066c-1.543.94-3.31-.826-2.37-2.37a1.724 1.724 0 00-1.066-2.573c-1.756-.426-1.756-2.924 0-3.35a1.724 1.724 0 001.066-2.573c-.94-1.543.826-3.31 2.37-2.37.996.608 2.296.07 2.572-1.065z" />
            <path strokeLinecap="round" strokeLinejoin="round" d="M15 12a3 3 0 11-6 0 3 3 0 016 0z" />
          </svg>
        </button>

        {/* Close button */}
        <button
          onClick={onClose}
          className="rounded p-1 text-slate-400 hover:bg-slate-100 hover:text-slate-600"
          title="Close chat (Esc)"
        >
          <svg className="h-3.5 w-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
            <path strokeLinecap="round" strokeLinejoin="round" d="M6 18L18 6M6 6l12 12" />
          </svg>
        </button>
      </div>
    </div>
  )
}
