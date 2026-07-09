import { useState } from 'react'
import { History, Loader2 } from 'lucide-react'
import { Button } from '../ui/button'
import { Popover, PopoverContent, PopoverTrigger } from '../ui/popover'
import { useAssistantSessions } from '../../hooks/useAssistant'
import { relativeTime } from '../../lib/utils'
import { useAssistantStore } from '../../store/assistantStore'
import type { UsageTotals } from '../../types/assistant'

// ── SessionHistory — archived-chats popover (claude provider) ──
//
// Header icon button listing ARCHIVED claude sessions, MRU first. Clicking
// one reopens it as a tab (`openSessionTab`); the panel's per-tab hydration
// then loads its transcript via `GET /session?session_id=`, and the
// backend reopens the row (archived → active) on the next message. The
// query is enabled only while the popover is open, so the list is fresh on
// every open.

function formatTokens(n: number): string {
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`
  if (n >= 1_000) return `${(n / 1_000).toFixed(1)}k`
  return String(n)
}

function usageSummary(usage: UsageTotals | null | undefined): string | null {
  if (usage == null) return null
  const total = (usage.input_tokens ?? 0) + (usage.output_tokens ?? 0)
  if (total === 0) return null
  return `${formatTokens(total)} tokens`
}

export function SessionHistory() {
  const [open, setOpen] = useState(false)
  const sessions = useAssistantSessions({ enabled: open })
  const openSessionTab = useAssistantStore((s) => s.openSessionTab)

  const archived = (sessions.data?.sessions ?? []).filter(
    (s) => s.provider === 'claude_sdk' && s.status === 'archived',
  )

  return (
    <Popover open={open} onOpenChange={setOpen}>
      <PopoverTrigger asChild>
        <Button
          variant="ghost"
          size="icon-sm"
          aria-label="Chat history"
          title="Chat history"
          className="text-muted-foreground"
        >
          <History aria-hidden="true" />
        </Button>
      </PopoverTrigger>
      <PopoverContent align="end" className="w-72 p-1">
        <p className="px-2 py-1.5 text-2xs font-medium uppercase tracking-wide text-muted-foreground">
          Chat history
        </p>
        {open && sessions.isPending ? (
          <div className="flex justify-center p-3">
            <Loader2
              role="status"
              aria-label="Loading history"
              className="size-4 animate-spin text-muted-foreground"
            />
          </div>
        ) : archived.length === 0 ? (
          <p className="px-2 pb-2 text-xs text-muted-foreground">
            No archived chats yet — closed tabs land here.
          </p>
        ) : (
          <ul className="max-h-80 overflow-y-auto">
            {archived.map((session) => {
              const usage = usageSummary(session.usage_totals)
              return (
                <li key={session.session_id}>
                  <button
                    onClick={() => {
                      openSessionTab(session.session_id, session.title ?? null)
                      setOpen(false)
                    }}
                    className="w-full rounded px-2 py-1.5 text-left text-xs hover:bg-accent"
                  >
                    <span className="block truncate text-foreground">
                      {session.title ?? 'Untitled chat'}
                    </span>
                    <span className="block text-2xs text-muted-foreground">
                      {relativeTime(session.last_used_at)}
                      {usage !== null ? ` · ${usage}` : ''}
                    </span>
                  </button>
                </li>
              )
            })}
          </ul>
        )}
      </PopoverContent>
    </Popover>
  )
}
