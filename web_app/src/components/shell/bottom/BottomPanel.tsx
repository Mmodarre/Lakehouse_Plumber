import { useCallback, type PointerEvent as ReactPointerEvent } from 'react'
import { ChevronDown, ChevronUp, CircleCheck } from 'lucide-react'
import { cn } from '../../../lib/utils'
import { useLayoutStore } from '../../../store/layoutStore'
import type { BottomTab } from '../../../store/layoutStore'
import { useRunStore } from '../../../store/runStore'
import { useHydrateProblems } from '../../../hooks/useHydrateProblems'
import { EmptyState } from '../../common/EmptyState'
import { ProblemsPanel } from '../../validation/ProblemsPanel'
import { RunStreamView } from './RunStreamView'
import { RunHistoryView } from './RunHistoryView'

// ── BottomPanel — Problems | Run | History dock (§3 / D12) ──────
//
// Collapsible bottom dock driven by layoutStore.bottomTab / bottomCollapsed /
// bottomHeight. The tab strip (with the collapse toggle) is always visible;
// clicking a tab while collapsed expands the dock onto it. A top-edge drag
// handle resizes bottomHeight (clamped 120–600px). Panes recompose existing
// surfaces: Problems = ProblemsPanel, Run = RunStreamView (← ValidationPanel),
// History = RunHistoryView (← RunHistoryPage table). AppShell owns the outer
// row height (bottomCollapsed ? strip : bottomHeight); this fills it.

const MIN_HEIGHT = 120
const MAX_HEIGHT = 600

/** Top-edge drag handle: dragging up grows the panel, clamped to bounds. */
function ResizeHandle() {
  const setBottomHeight = useLayoutStore((s) => s.setBottomHeight)
  const onPointerDown = useCallback(
    (e: ReactPointerEvent) => {
      e.preventDefault()
      const startY = e.clientY
      const startHeight = useLayoutStore.getState().bottomHeight
      const onMove = (ev: PointerEvent) => {
        const next = startHeight + (startY - ev.clientY)
        setBottomHeight(Math.min(MAX_HEIGHT, Math.max(MIN_HEIGHT, next)))
      }
      const onUp = () => {
        window.removeEventListener('pointermove', onMove)
        window.removeEventListener('pointerup', onUp)
      }
      window.addEventListener('pointermove', onMove)
      window.addEventListener('pointerup', onUp)
    },
    [setBottomHeight],
  )
  return (
    <div
      role="separator"
      aria-orientation="horizontal"
      aria-label="Resize panel"
      onPointerDown={onPointerDown}
      className="h-1 shrink-0 cursor-row-resize bg-transparent transition-colors hover:bg-primary/40"
    />
  )
}

function TabButton({
  active,
  label,
  count,
  live,
  onClick,
}: {
  active: boolean
  label: string
  count?: number
  live?: boolean
  onClick: () => void
}) {
  return (
    <button
      type="button"
      role="tab"
      aria-selected={active}
      onClick={onClick}
      className={cn(
        'flex h-7 items-center gap-1.5 border-b-2 px-2.5 text-xs font-medium transition-colors',
        active
          ? 'border-primary text-foreground'
          : 'border-transparent text-muted-foreground hover:text-foreground',
      )}
    >
      {label}
      {count !== undefined && count > 0 && (
        <span className="flex min-w-4 items-center justify-center rounded-full bg-muted px-1 text-[10px] leading-none text-muted-foreground tabular-nums">
          {count}
        </span>
      )}
      {live && (
        <span className="flex items-center gap-1 text-[10px] font-semibold tracking-wide text-primary uppercase">
          <span className="size-1.5 animate-pulse rounded-full bg-primary" aria-hidden="true" />
          live
        </span>
      )}
    </button>
  )
}

function ProblemsPane() {
  const issueCount = useRunStore((s) => s.issues.length)
  // ProblemsPanel stays mounted across empty↔populated so its persistent
  // sr-only role="status" region reliably ANNOUNCES count changes to screen
  // readers (a region that unmounts, or one that mounts already holding
  // content, is never announced). ProblemsPanel renders nothing visible when
  // there are no issues, so the EmptyState below supplies the zero-state
  // visual without duplicating the live region.
  return (
    <div className="h-full overflow-auto p-2">
      <ProblemsPanel />
      {issueCount === 0 && (
        <EmptyState
          icon={CircleCheck}
          title="No problems"
          message="Validate the project to surface issues here."
        />
      )}
    </div>
  )
}

export function BottomPanel() {
  // One-shot: restore the last validation's Problems on boot (§6.7).
  useHydrateProblems()
  const collapsed = useLayoutStore((s) => s.bottomCollapsed)
  const setCollapsed = useLayoutStore((s) => s.setBottomCollapsed)
  const tab = useLayoutStore((s) => s.bottomTab)
  const setTab = useLayoutStore((s) => s.setBottomTab)
  const issueCount = useRunStore((s) => s.issues.length)
  const isRunning = useRunStore((s) => s.isRunning)

  const selectTab = useCallback(
    (next: BottomTab) => {
      setTab(next)
      if (useLayoutStore.getState().bottomCollapsed) setCollapsed(false)
    },
    [setTab, setCollapsed],
  )

  return (
    <div className="flex h-full w-full min-w-0 flex-col overflow-hidden">
      {!collapsed && <ResizeHandle />}
      <div
        role="tablist"
        aria-label="Panel"
        className="flex h-7 shrink-0 items-center gap-0.5 border-b border-border px-1.5"
      >
        <TabButton
          active={!collapsed && tab === 'problems'}
          label="Problems"
          count={issueCount}
          onClick={() => selectTab('problems')}
        />
        <TabButton
          active={!collapsed && tab === 'run'}
          label="Run"
          live={isRunning}
          onClick={() => selectTab('run')}
        />
        <TabButton
          active={!collapsed && tab === 'history'}
          label="History"
          onClick={() => selectTab('history')}
        />
        <button
          type="button"
          onClick={() => setCollapsed(!collapsed)}
          aria-label={collapsed ? 'Expand panel' : 'Collapse panel'}
          title={collapsed ? 'Expand panel' : 'Collapse panel'}
          className="ml-auto flex size-6 items-center justify-center rounded-md text-muted-foreground transition-colors hover:bg-muted/60 hover:text-foreground"
        >
          {collapsed ? (
            <ChevronUp className="size-4" aria-hidden="true" />
          ) : (
            <ChevronDown className="size-4" aria-hidden="true" />
          )}
        </button>
      </div>
      {!collapsed && (
        <div className="min-h-0 flex-1 overflow-hidden">
          {tab === 'problems' && <ProblemsPane />}
          {tab === 'run' && <RunStreamView />}
          {tab === 'history' && <RunHistoryView />}
        </div>
      )}
    </div>
  )
}
