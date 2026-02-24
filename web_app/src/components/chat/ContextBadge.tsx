/** Shows the auto-detected context in the chat input area. */

import type { ChatContext } from '../../types/chat'

const icons: Record<string, string> = {
  file: 'M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z',
  flowgroup: 'M19 11H5m14 0a2 2 0 012 2v6a2 2 0 01-2 2H5a2 2 0 01-2-2v-6a2 2 0 012-2m14 0V9a2 2 0 00-2-2M5 11V9a2 2 0 012-2m0 0V5a2 2 0 012-2h6a2 2 0 012 2v2M7 7h10',
  page: 'M3 7v10a2 2 0 002 2h14a2 2 0 002-2V9a2 2 0 00-2-2h-6l-2-2H5a2 2 0 00-2 2z',
}

export function ContextBadge({ context }: { context: ChatContext }) {
  if (!context) return null

  return (
    <div className="flex items-center gap-1 rounded bg-slate-50 px-2 py-0.5 text-[10px] text-slate-500">
      <svg className="h-3 w-3" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1.5}>
        <path strokeLinecap="round" strokeLinejoin="round" d={icons[context.type] ?? icons.page} />
      </svg>
      <span className="max-w-[180px] truncate">{context.name}</span>
    </div>
  )
}
