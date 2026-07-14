import { useEffect, useRef } from 'react'
import type { LucideIcon } from 'lucide-react'
import { Code2, Waypoints } from 'lucide-react'

import { cn } from '@/lib/utils'
import {
  useWorkspaceStore,
  workspaceTabId,
  type EntityTab,
  type EntityView,
} from '@/store/workspaceStore'

// ── EntityHeader — per-entity breadcrumb + Graph|Code switcher (§6.2/§6.4) ────
//
// The center header above an entity tab's body: a breadcrumb (pipeline ▸
// flowgroup, or Template ▸ name) and a segmented view switcher. The switcher is
// an ARIA tablist with roving tabindex (arrow keys move + select; ⌘1/2 select
// Graph/Code). Selecting a view calls workspaceStore.setTabView — both views
// are the SAME live-synced document (D2), so switching never loses edits
// (CenterArea flushes the outgoing Monaco buffer on the transition).

interface ViewOption {
  key: EntityView
  label: string
  Icon: LucideIcon
}

const VIEWS: readonly ViewOption[] = [
  { key: 'graph', label: 'Graph', Icon: Waypoints },
  { key: 'code', label: 'Code', Icon: Code2 },
]

export function EntityHeader({ tab }: { tab: EntityTab }) {
  const setTabView = useWorkspaceStore((s) => s.setTabView)
  const tabId = workspaceTabId(tab)
  const view = tab.view
  const isTemplate = tab.docKind === 'template'
  const tablistRef = useRef<HTMLDivElement>(null)

  // ⌘1 / ⌘2 (or Ctrl on non-mac) select Graph / Code (§6.4).
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (!(e.metaKey || e.ctrlKey) || e.altKey || e.shiftKey) return
      const idx = e.key === '1' ? 0 : e.key === '2' ? 1 : -1
      if (idx === -1) return
      e.preventDefault()
      setTabView(tabId, VIEWS[idx].key)
    }
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
  }, [tabId, setTabView])

  const focusTab = (idx: number) => {
    const btns = tablistRef.current?.querySelectorAll<HTMLButtonElement>('[role="tab"]')
    btns?.[idx]?.focus()
  }

  const onTablistKeyDown = (e: React.KeyboardEvent) => {
    if (e.key !== 'ArrowRight' && e.key !== 'ArrowLeft') return
    e.preventDefault()
    const current = VIEWS.findIndex((v) => v.key === view)
    const dir = e.key === 'ArrowRight' ? 1 : -1
    const nextIdx = (current + dir + VIEWS.length) % VIEWS.length
    setTabView(tabId, VIEWS[nextIdx].key)
    focusTab(nextIdx)
  }

  const crumbLead = isTemplate ? 'Template' : tab.pipeline
  const crumbName = tab.flowgroup

  return (
    <div className="flex items-center justify-between gap-2 border-b border-border bg-card px-4 py-1.5">
      <nav aria-label="Breadcrumb" className="flex min-w-0 items-center gap-1.5 text-2xs">
        {crumbLead !== '' && (
          <>
            <span className="truncate font-medium uppercase tracking-[0.05em] text-muted-foreground">
              {crumbLead}
            </span>
            <span className="text-faint" aria-hidden="true">
              /
            </span>
          </>
        )}
        <span className="truncate font-mono text-xs font-semibold text-foreground">{crumbName}</span>
      </nav>

      <div
        ref={tablistRef}
        role="tablist"
        aria-label="Entity view"
        onKeyDown={onTablistKeyDown}
        className="flex shrink-0 items-center gap-0.5 rounded-sm border border-border bg-background p-0.5"
      >
        {VIEWS.map(({ key, label, Icon }) => {
          const selected = view === key
          return (
            <button
              key={key}
              type="button"
              role="tab"
              aria-selected={selected}
              tabIndex={selected ? 0 : -1}
              onClick={() => setTabView(tabId, key)}
              className={cn(
                'flex items-center gap-1 rounded-[3px] px-2 py-0.5 text-2xs font-medium transition-colors duration-150',
                'outline-none focus-visible:ring-2 focus-visible:ring-inset focus-visible:ring-ring/60',
                selected
                  ? 'bg-accent-weak text-primary'
                  : 'text-muted-foreground hover:bg-muted hover:text-foreground',
              )}
            >
              <Icon className="size-3" aria-hidden="true" />
              {label}
            </button>
          )
        })}
      </div>
    </div>
  )
}
