import { tablistKeyDown } from '@/lib/keyboard'
import { lazy, Suspense, useCallback, useState, type PointerEvent as ReactPointerEvent } from 'react'
import { ChevronDown, ChevronUp, CircleCheck, CircleHelp } from 'lucide-react'
import { cn } from '../../../lib/utils'
import { useLayoutStore } from '../../../store/layoutStore'
import type { BottomTab } from '../../../store/layoutStore'
import { useWorkspaceStore } from '../../../store/workspaceStore'
import { useRunStore } from '../../../store/runStore'
import { useHydrateProblems } from '../../../hooks/useHydrateProblems'
import { EmptyState } from '../../common/EmptyState'
import { ProblemsPanel } from '../../validation/ProblemsPanel'
const RunStreamView = lazy(() => import('./RunStreamView').then((m) => ({ default: m.RunStreamView })))
const RunHistoryView = lazy(() => import('./RunHistoryView').then((m) => ({ default: m.RunHistoryView })))

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
      tabIndex={0}
      aria-valuemin={MIN_HEIGHT}
      aria-valuemax={MAX_HEIGHT}
      aria-valuenow={useLayoutStore.getState().bottomHeight}
      onKeyDown={(e) => { if (e.key === 'ArrowUp' || e.key === 'ArrowDown') { e.preventDefault(); setBottomHeight(useLayoutStore.getState().bottomHeight + (e.key === 'ArrowUp' ? 20 : -20)) } }}
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
      tabIndex={active ? 0 : -1}
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
  const { issues, isRunning, runKind, terminal, hydratedFrom } = useRunStore()
  const issueCount = issues.length
  const dirty = useWorkspaceStore((s) => s.buffers.some((buffer) => buffer.isDirty))
  const empty = dirty
    ? { title: 'Results need refresh', message: 'Save and validate to include your current edits.' }
    : isRunning
      ? { title: runKind === 'generate' ? 'Generation in progress' : 'Validation in progress', message: 'Diagnostics will appear as the run progresses.' }
      : runKind === 'validate' && terminal === 'success'
        ? { title: 'No problems', message: 'Saved files passed validation.' }
        : hydratedFrom
          ? { title: 'No recorded problems', message: 'Previous validation recorded no diagnostics. Validate again to check current saved files.' }
          : terminal && terminal !== 'success'
            ? { title: `Run ${terminal}`, message: 'No diagnostics were recorded. Validate again to check saved files.' }
            : { title: 'Not validated', message: 'Validate the saved project to check for issues.' }
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
          icon={runKind === 'validate' && terminal === 'success' && !dirty ? CircleCheck : CircleHelp}
          title={empty.title}
          message={empty.message}
        />
      )}
    </div>
  )
}

export function BottomPanel({ forceCollapsed = false }: { forceCollapsed?: boolean }) {
  // One-shot: restore the last validation's Problems on boot (§6.7).
  useHydrateProblems()
  const storedCollapsed = useLayoutStore((s) => s.bottomCollapsed)
  const collapsed = forceCollapsed || storedCollapsed
  const setCollapsed = useLayoutStore((s) => s.setBottomCollapsed)
  const tab = useLayoutStore((s) => s.bottomTab)
  const setTab = useLayoutStore((s) => s.setBottomTab)
  const [visited, setVisited] = useState<BottomTab[]>([tab])
  // Commands can activate panes without passing through this tab strip.
  if (!visited.includes(tab)) setVisited([...visited, tab])
  const issueCount = useRunStore((s) => s.issues.length)
  const isRunning = useRunStore((s) => s.isRunning)

  const selectTab = useCallback(
    (next: BottomTab) => {
      if (forceCollapsed && useLayoutStore.getState().focusMode) useLayoutStore.getState().toggleFocusMode()
      setVisited((previous) => previous.includes(next) ? previous : [...previous, next])
      setTab(next)
      if (useLayoutStore.getState().bottomCollapsed) setCollapsed(false)
    },
    [setTab, setCollapsed, forceCollapsed],
  )

  return (
    <div className="flex h-full w-full min-w-0 flex-col overflow-hidden">
      {!collapsed && <ResizeHandle />}
      <div
        role="tablist"
        onKeyDown={tablistKeyDown}
        aria-label="Panel"
        className="flex h-7 shrink-0 items-center gap-0.5 border-b border-border px-1.5"
      >
        <TabButton
          active={tab === 'problems'}
          label="Problems"
          count={issueCount}
          onClick={() => selectTab('problems')}
        />
        <TabButton
          active={tab === 'run'}
          label="Run"
          live={isRunning}
          onClick={() => selectTab('run')}
        />
        <TabButton
          active={tab === 'history'}
          label="History"
          onClick={() => selectTab('history')}
        />
        <button
          type="button"
          onClick={() => {
            if (forceCollapsed && useLayoutStore.getState().focusMode) useLayoutStore.getState().toggleFocusMode()
            setCollapsed(!collapsed)
          }}
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
      <div hidden={collapsed} className="min-h-0 flex-1 overflow-hidden">
        {(visited.includes('problems') || tab === 'problems') && <div hidden={tab !== 'problems'} className="h-full"><ProblemsPane /></div>}
        {(visited.includes('run') || tab === 'run') && <div hidden={tab !== 'run'} className="h-full"><Suspense fallback={<p className="p-3 text-xs text-muted-foreground" role="status">Loading run details…</p>}><RunStreamView /></Suspense></div>}
        {(visited.includes('history') || tab === 'history') && <div hidden={tab !== 'history'} className="h-full"><Suspense fallback={<p className="p-3 text-xs text-muted-foreground" role="status">Loading history…</p>}><RunHistoryView /></Suspense></div>}
      </div>
    </div>
  )
}
