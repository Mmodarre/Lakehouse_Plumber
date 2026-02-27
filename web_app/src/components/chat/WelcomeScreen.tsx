/**
 * Welcome screen — shown when no sessions are open (no tabs).
 *
 * Displays new-chat buttons (Agent / Chat) and a dropdown
 * to resume previous sessions from backend history.
 */

import { useState, useRef, useEffect } from 'react'
import { relativeTime } from '../../utils/timeFormat'
import type { SessionMode, SessionSummary } from '../../types/chat'

export function WelcomeScreen({
  onNewChat,
  availableSessions,
  onOpenSession,
  isLoadingHistory,
}: {
  onNewChat: (mode: SessionMode) => void
  availableSessions: SessionSummary[]
  onOpenSession: (session: SessionSummary) => void
  isLoadingHistory: boolean
}) {
  const [dropdownOpen, setDropdownOpen] = useState(false)
  const dropdownRef = useRef<HTMLDivElement>(null)

  // Close dropdown on outside click
  useEffect(() => {
    if (!dropdownOpen) return
    function handleClickOutside(e: MouseEvent) {
      if (dropdownRef.current && !dropdownRef.current.contains(e.target as Node)) {
        setDropdownOpen(false)
      }
    }
    document.addEventListener('mousedown', handleClickOutside)
    return () => document.removeEventListener('mousedown', handleClickOutside)
  }, [dropdownOpen])

  return (
    <div className="flex flex-1 flex-col items-center justify-center px-6">
      {/* Icon */}
      <div className="mb-3 flex h-10 w-10 items-center justify-center rounded-full bg-blue-100">
        <svg className="h-5 w-5 text-blue-600" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1.5}>
          <path strokeLinecap="round" strokeLinejoin="round" d="M8.625 12a.375.375 0 11-.75 0 .375.375 0 01.75 0zm4.125 0a.375.375 0 11-.75 0 .375.375 0 01.75 0zm4.125 0a.375.375 0 11-.75 0 .375.375 0 01.75 0z" />
          <path strokeLinecap="round" strokeLinejoin="round" d="M12 2.25c-5.385 0-9.75 4.365-9.75 9.75s4.365 9.75 9.75 9.75 9.75-4.365 9.75-9.75S17.385 2.25 12 2.25z" />
        </svg>
      </div>

      <h3 className="mb-1 text-sm font-medium text-slate-700">Start a conversation</h3>
      <p className="mb-5 text-center text-[11px] text-slate-400">
        Choose a mode to begin or resume a previous session
      </p>

      {/* New chat buttons */}
      <div className="mb-5 flex gap-2">
        <button
          onClick={() => onNewChat('agent')}
          className="flex items-center gap-1.5 rounded-md border border-slate-200 bg-white px-3 py-1.5 text-[11px] font-medium text-slate-700 shadow-sm hover:border-blue-300 hover:bg-blue-50 transition-colors"
        >
          <span className="flex h-4 w-4 items-center justify-center rounded text-[9px] font-bold text-white bg-blue-500">A</span>
          New Agent Chat
        </button>
        <button
          onClick={() => onNewChat('chat')}
          className="flex items-center gap-1.5 rounded-md border border-slate-200 bg-white px-3 py-1.5 text-[11px] font-medium text-slate-700 shadow-sm hover:border-emerald-300 hover:bg-emerald-50 transition-colors"
        >
          <span className="flex h-4 w-4 items-center justify-center rounded text-[9px] font-bold text-white bg-emerald-500">C</span>
          New Chat
        </button>
      </div>

      {/* Divider */}
      <div className="mb-4 flex w-full items-center gap-2">
        <div className="h-px flex-1 bg-slate-200" />
        <span className="text-[10px] text-slate-400">or resume</span>
        <div className="h-px flex-1 bg-slate-200" />
      </div>

      {/* Previous sessions dropdown */}
      <div className="relative w-full max-w-[240px]" ref={dropdownRef}>
        <button
          onClick={() => setDropdownOpen((prev) => !prev)}
          className="flex w-full items-center justify-between rounded-md border border-slate-200 bg-white px-3 py-1.5 text-[11px] text-slate-600 shadow-sm hover:border-slate-300 transition-colors"
        >
          <span>{isLoadingHistory ? 'Loading...' : 'Previous sessions'}</span>
          <svg
            className={`h-3 w-3 text-slate-400 transition-transform ${dropdownOpen ? 'rotate-180' : ''}`}
            fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}
          >
            <path strokeLinecap="round" strokeLinejoin="round" d="M19 9l-7 7-7-7" />
          </svg>
        </button>

        {dropdownOpen && (
          <div className="absolute left-0 top-full z-50 mt-1 w-full rounded-md border border-slate-200 bg-white py-1 shadow-lg">
            {availableSessions.length === 0 ? (
              <div className="px-3 py-2 text-[11px] text-slate-400">
                No previous sessions
              </div>
            ) : (
              <div className="max-h-[200px] overflow-y-auto">
                {availableSessions.map((session) => {
                  const mode = session.mode ?? 'agent'
                  const modeIcon = mode === 'chat' ? 'C' : 'A'
                  const modeBg = mode === 'chat' ? 'bg-emerald-500' : 'bg-blue-500'
                  return (
                    <button
                      key={session.id}
                      onClick={() => {
                        setDropdownOpen(false)
                        onOpenSession(session)
                      }}
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
    </div>
  )
}
