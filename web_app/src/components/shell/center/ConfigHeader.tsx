import { useEffect, useRef } from 'react'
import type { LucideIcon } from 'lucide-react'
import { Code2, SquarePen } from 'lucide-react'

import { cn } from '@/lib/utils'
import {
  useWorkspaceStore,
  workspaceTabId,
  type ConfigKind,
  type ConfigTab,
  type ConfigView,
} from '@/store/workspaceStore'

// ── ConfigHeader — per-config breadcrumb + Form|YAML switcher (§6.2/§6.4) ──
//
// The center header above a config tab's body: a breadcrumb (surface label ▸
// file name) and a segmented view switcher. Config surfaces get Form | YAML
// only — there is no Graph view (§6.2). Mirrors EntityHeader's interaction
// contract: an ARIA tablist with roving tabindex (arrow keys move + select;
// ⌘1/⌘2 select Form/YAML). Both views are the SAME live-synced document (D2),
// so switching never loses edits (CenterArea flushes the outgoing Monaco
// buffer on the transition).

interface ViewOption {
  key: ConfigView
  label: string
  Icon: LucideIcon
}

const VIEWS: readonly ViewOption[] = [
  { key: 'form', label: 'Form', Icon: SquarePen },
  { key: 'yaml', label: 'YAML', Icon: Code2 },
]

const KIND_LABEL: Record<ConfigKind, string> = {
  project: 'Project',
  pipeline: 'Pipeline',
  job: 'Job',
}

export function ConfigHeader({ tab }: { tab: ConfigTab }) {
  const setTabView = useWorkspaceStore((s) => s.setTabView)
  const tabId = workspaceTabId(tab)
  const view = tab.view
  const tablistRef = useRef<HTMLDivElement>(null)

  // ⌘1 / ⌘2 (or Ctrl on non-mac) select Form / YAML (§6.4 — no Graph for config).
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

  const crumbName = tab.path.split('/').pop() ?? tab.path

  return (
    <div className="flex items-center justify-between gap-2 border-b border-border bg-card px-4 py-1.5">
      <nav aria-label="Breadcrumb" className="flex min-w-0 items-center gap-1.5 text-2xs">
        <span className="truncate font-medium uppercase tracking-[0.05em] text-muted-foreground">
          {KIND_LABEL[tab.configKind]}
        </span>
        <span className="text-faint" aria-hidden="true">
          /
        </span>
        <span className="truncate font-mono text-xs font-semibold text-foreground">{crumbName}</span>
      </nav>

      <div
        ref={tablistRef}
        role="tablist"
        aria-label="Config view"
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
