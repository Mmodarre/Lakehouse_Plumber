/**
 * Session tab bar — renders below the ChatPanelHeader.
 *
 * Hidden when there are 0-1 sessions to avoid visual noise.
 * Each tab shows: mode icon (A/C), truncated title, unread badge,
 * and a close (X) button on hover.
 */

import { useChatStore } from '../../store/chatStore'
import type { SessionSummary } from '../../types/chat'

export function SessionTabBar() {
  const sessions = useChatStore((s) => s.sessions)
  const activeSessionId = useChatStore((s) => s.activeSessionId)
  const setActiveSession = useChatStore((s) => s.setActiveSession)
  const closeTab = useChatStore((s) => s.closeTab)
  const sessionModes = useChatStore((s) => s.sessionModes)

  // Hide when 0-1 sessions — no tab bar needed
  if (sessions.length <= 1) return null

  const handleClose = (e: React.MouseEvent, session: SessionSummary) => {
    e.stopPropagation()
    closeTab(session.id)
  }

  return (
    <div className="flex items-center gap-0.5 overflow-x-auto border-b border-slate-200 bg-white px-1 scrollbar-none">
      {sessions.map((session) => {
        const isActive = session.id === activeSessionId
        const mode = sessionModes[session.id] ?? session.mode ?? 'agent'
        const modeIcon = mode === 'chat' ? 'C' : 'A'
        const modeColor = mode === 'chat' ? 'text-emerald-600' : 'text-blue-600'

        return (
          <button
            key={session.id}
            onClick={() => setActiveSession(session.id)}
            className={`group relative flex shrink-0 items-center gap-1 rounded-t px-2 py-1.5 text-[10px] transition-colors ${
              isActive
                ? 'border-b-2 border-blue-500 bg-slate-50 text-blue-700'
                : 'text-slate-500 hover:bg-slate-50 hover:text-slate-700'
            }`}
            title={session.title}
          >
            {/* Mode icon */}
            <span className={`font-bold ${modeColor}`}>{modeIcon}</span>

            {/* Truncated title */}
            <span className="max-w-[80px] truncate">
              {session.title || 'Untitled'}
            </span>

            {/* Unread badge */}
            {session.unreadCount > 0 && !isActive && (
              <span className="flex h-3.5 min-w-[14px] items-center justify-center rounded-full bg-blue-500 px-1 text-[8px] font-bold text-white">
                {session.unreadCount > 9 ? '9+' : session.unreadCount}
              </span>
            )}

            {/* Close button (visible on hover) */}
            <span
              onClick={(e) => handleClose(e, session)}
              className="ml-0.5 hidden rounded p-0.5 text-slate-400 hover:bg-slate-200 hover:text-slate-600 group-hover:inline-flex"
              title="Close session"
            >
              <svg className="h-2.5 w-2.5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2.5}>
                <path strokeLinecap="round" strokeLinejoin="round" d="M6 18L18 6M6 6l12 12" />
              </svg>
            </span>
          </button>
        )
      })}
    </div>
  )
}
