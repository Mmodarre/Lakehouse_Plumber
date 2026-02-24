/** Chat panel header with session info, settings gear, and close button. */

import type { ConnectionStatus } from '../../types/chat'

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
}: {
  connectionStatus: ConnectionStatus
  onClose: () => void
  onSettings: () => void
}) {
  return (
    <div className="flex h-10 items-center border-b border-slate-200 bg-white px-3">
      {/* Status dot + title */}
      <div className="flex items-center gap-2">
        <div className={`h-1.5 w-1.5 rounded-full ${statusColors[connectionStatus]}`} />
        <span className="text-[11px] font-medium text-slate-700">AI Assistant</span>
      </div>

      <div className="ml-auto flex items-center gap-1">
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
