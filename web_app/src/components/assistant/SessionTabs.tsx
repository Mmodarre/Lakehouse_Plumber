import { ChevronDown, Loader2, Plus, X } from 'lucide-react'
import { Button } from '../ui/button'
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from '../ui/dropdown-menu'
import { useArchiveSession } from '../../hooks/useAssistant'
import { isDraftKey, useAssistantStore } from '../../store/assistantStore'
import type { ConversationState } from '../../store/assistantStore'
import { cn } from '../../lib/utils'

// ── SessionTabs — the claude provider's tab strip ──────────────
//
// One tab per open conversation (active session or unsent draft). Titles
// come from the server list (`tabTitles`, set once from the first user
// message), falling back to the conversation's own first user message
// (covers the send→refetch gap), then "New chat". A simple count-based
// cutoff sends the rest into an overflow dropdown; the ACTIVE tab is always
// kept visible by swapping it into the last slot.

const MAX_VISIBLE_TABS = 4

function firstUserText(conversation: ConversationState | undefined): string | null {
  const part = conversation?.parts.find(
    (p) => p.kind === 'text' && p.role === 'user',
  )
  return part !== undefined && part.kind === 'text' ? part.text : null
}

export function SessionTabs() {
  const tabOrder = useAssistantStore((s) => s.tabOrder)
  const activeTabKey = useAssistantStore((s) => s.activeTabKey)
  const conversations = useAssistantStore((s) => s.conversations)
  const tabTitles = useAssistantStore((s) => s.tabTitles)
  const activateTab = useAssistantStore((s) => s.activateTab)
  const closeTab = useAssistantStore((s) => s.closeTab)
  const openTab = useAssistantStore((s) => s.openTab)
  const archive = useArchiveSession()

  const titleOf = (key: string): string =>
    tabTitles[key] ?? firstUserText(conversations[key]) ?? 'New chat'

  const close = (key: string) => {
    // Draft tabs exist only client-side; real sessions archive server-side.
    if (!isDraftKey(key)) archive.mutate(key)
    closeTab(key)
  }

  let visible = tabOrder.slice(0, MAX_VISIBLE_TABS)
  let overflow = tabOrder.slice(MAX_VISIBLE_TABS)
  if (activeTabKey !== null && overflow.includes(activeTabKey)) {
    const displaced = visible[visible.length - 1]
    visible = [...visible.slice(0, -1), activeTabKey]
    overflow = overflow.map((k) => (k === activeTabKey ? displaced : k))
  }

  return (
    <div
      role="tablist"
      aria-label="Chat sessions"
      className="flex h-8 shrink-0 items-center gap-0.5 border-b border-border px-1"
    >
      {visible.map((key) => {
        const isActive = key === activeTabKey
        const streaming = conversations[key]?.streaming === true
        const title = titleOf(key)
        return (
          <div
            key={key}
            className={cn(
              'group flex min-w-0 max-w-36 flex-1 items-center rounded-t border-b-2 text-xs',
              isActive
                ? 'border-primary bg-accent/50 text-foreground'
                : 'border-transparent text-muted-foreground hover:bg-accent/30 hover:text-foreground',
            )}
          >
            <button
              role="tab"
              aria-selected={isActive}
              onClick={() => activateTab(key)}
              title={title}
              className="flex min-w-0 flex-1 items-center gap-1 px-1.5 py-1"
            >
              {streaming && (
                <Loader2
                  role="status"
                  aria-label="Turn running"
                  className="size-3 shrink-0 animate-spin"
                />
              )}
              <span className="truncate">{title}</span>
            </button>
            <button
              onClick={() => close(key)}
              aria-label={`Close ${title}`}
              title="Close tab"
              className="mr-0.5 shrink-0 rounded p-0.5 opacity-0 hover:bg-accent focus-visible:opacity-100 group-hover:opacity-100"
            >
              <X className="size-3" aria-hidden="true" />
            </button>
          </div>
        )
      })}
      {overflow.length > 0 && (
        <DropdownMenu>
          <DropdownMenuTrigger asChild>
            <Button
              variant="ghost"
              size="icon-sm"
              aria-label={`${overflow.length} more tabs`}
              title="More tabs"
              className="shrink-0 text-muted-foreground"
            >
              <ChevronDown aria-hidden="true" />
            </Button>
          </DropdownMenuTrigger>
          <DropdownMenuContent align="end">
            {overflow.map((key) => (
              <DropdownMenuItem key={key} onSelect={() => activateTab(key)}>
                {conversations[key]?.streaming === true && (
                  <Loader2
                    className="size-3 animate-spin"
                    aria-hidden="true"
                  />
                )}
                <span className="max-w-52 truncate text-xs">{titleOf(key)}</span>
              </DropdownMenuItem>
            ))}
          </DropdownMenuContent>
        </DropdownMenu>
      )}
      <Button
        variant="ghost"
        size="icon-sm"
        onClick={() => openTab()}
        aria-label="New chat tab"
        title="New chat tab"
        className="shrink-0 text-muted-foreground"
      >
        <Plus aria-hidden="true" />
      </Button>
    </div>
  )
}
