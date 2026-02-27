/** Connection status banner shown when not connected or reconnecting. */

import type { ConnectionStatus as Status } from '../../types/chat'

const statusConfig: Record<Status, { text: string; bg: string; show: boolean }> = {
  disconnected: { text: 'AI disconnected', bg: 'bg-slate-100 text-slate-500', show: true },
  connecting: { text: 'Connecting to AI...', bg: 'bg-blue-50 text-blue-600', show: true },
  connected: { text: '', bg: '', show: false },
  reconnecting: { text: 'Reconnecting...', bg: 'bg-amber-50 text-amber-600', show: true },
}

export function ConnectionStatusBanner({
  status,
  streamingStalled,
}: {
  status: Status
  streamingStalled?: boolean
}) {
  const config = statusConfig[status]

  if (streamingStalled) {
    return (
      <div className="bg-amber-50 px-3 py-1.5 text-center text-[10px] font-medium text-amber-600">
        Still waiting for response... You can stop generation with {navigator.platform.includes('Mac') ? 'Cmd' : 'Ctrl'}+.
      </div>
    )
  }

  if (!config.show) return null

  return (
    <div className={`px-3 py-1.5 text-center text-[10px] font-medium ${config.bg}`}>
      {config.text}
    </div>
  )
}
