import { useCallback } from 'react'
import { openWorkspaceFile } from '@/workspace/openWorkspaceFile'
import { tablistKeyDown } from '@/lib/keyboard'
import {
  ChevronRight,
  CircleCheck,
  CircleHelp,
  CircleX,
  ClipboardCheck,
  TriangleAlert,
} from 'lucide-react'
import { cn } from '../../../lib/utils'
import { useLayoutStore } from '../../../store/layoutStore'
import type { InspectorTab } from '../../../store/layoutStore'
import { useRunStore } from '../../../store/runStore'
import { useWorkspaceStore, workspaceTabId } from '../../../store/workspaceStore'
import type { WorkspaceTabRef } from '../../../store/workspaceStore'
import type { ValidationIssue } from '../../../types/api'
import { IssueList } from '../../validation/IssueList'
import type { IssueListItem } from '../../validation/IssueList'
import { EmptyState } from '../../common/EmptyState'
import { scopeIssues } from './scopeIssues'
import type { ScopedIssues } from './scopeIssues'
import { mergeConfigIssues, useActiveConfigIssues } from './configIssues'

// ── Inspector — right-dock Validation | Help (§3 / §6.4) ─────────
//
// Two tabs driven by layoutStore.inspectorTab; collapses to a 42px vertical
// rail via inspectorCollapsed (the rail's icon buttons expand + select a tab).
// Action FIELD editing no longer lives here — it moved to a double-click modal
// on the graph node (Fix #3), so `inspectorTab` no longer admits 'action' (the
// layoutStore v2 migrate heals any stored 'action' → 'validation'). Validation
// recomposes the shared IssueList rows
// (same rendering as ProblemsPanel / run history) scoped to the active center
// tab; a row that carries a file opens that file as a workspace buffer —
// identical to ProblemsPanel's behaviour. Help is a static placeholder until
// the schema-help routing task wires it.

function toIssueItems(issues: ValidationIssue[]): IssueListItem[] {
  return issues.map((issue) => {
    const line = issue.context['line']
    return {
      severity: issue.severity,
      code: issue.code,
      message: issue.title,
      details: issue.details,
      suggestions: issue.suggestions,
      file: issue.file_path,
      line: typeof line === 'number' ? line : null,
    }
  })
}

/** Active center tab (or null when the workspace is on the page view). */
function useActiveTab(): WorkspaceTabRef | null {
  const tabs = useWorkspaceStore((s) => s.tabs)
  const activePath = useWorkspaceStore((s) => s.activePath)
  if (activePath === null) return null
  return tabs.find((t) => workspaceTabId(t) === activePath) ?? null
}

function ValidationPane({ scoped }: { scoped: ScopedIssues }) {
  const { issues, scope } = scoped
  const isRunning = useRunStore((s) => s.isRunning)
  const terminal = useRunStore((s) => s.terminal)
  const hydrated = useRunStore((s) => s.hydratedFrom)
  const runInputs = useRunStore((s) => s.inputs)
  const dirty = useWorkspaceStore((s) => s.buffers.some((b) => b.isDirty))
  const description = isRunning ? 'Validation is in progress; these results are incomplete.'
    : dirty ? 'Results out of date: unsaved edits have not been validated.'
    : terminal === 'stopped' || terminal === 'incomplete' || terminal === 'error' ? 'The last run did not complete. Validate again for current results.'
    : !terminal && !hydrated ? 'Not validated. Run Validate to check saved project files.'
    : `Saved-file results${runInputs?.env || hydrated?.env ? ` · ${runInputs?.env || hydrated?.env}` : ''}${runInputs?.pipeline || hydrated?.pipeline ? ` / ${runInputs?.pipeline || hydrated?.pipeline}` : ' · all pipelines'}.`
  const errorCount = issues.filter((i) => i.severity === 'error').length
  const warningCount = issues.length - errorCount

  if (issues.length === 0) {
    return (
      <EmptyState
        icon={CircleCheck}
        title="No issues"
        message={
          `${scope.projectWide ? '' : `No issues for ${scope.label}. `}${description}`
        }
      />
    )
  }

  return (
    <div className="flex min-h-0 flex-col">
      <p className="border-b border-border px-3 py-2 text-xs text-muted-foreground">{description}</p>
      <div className="flex items-center gap-2 border-b border-border px-3 py-1.5 text-2xs text-muted-foreground">
        {errorCount > 0 ? (
          <span className="flex items-center gap-1 text-error">
            <CircleX className="size-3" aria-hidden="true" />
            {errorCount} {errorCount === 1 ? 'error' : 'errors'}
          </span>
        ) : (
          <span className="flex items-center gap-1 text-success">
            <CircleCheck className="size-3" aria-hidden="true" />0 errors
          </span>
        )}
        {warningCount > 0 && (
          <span className="flex items-center gap-1 text-warning">
            <TriangleAlert className="size-3" aria-hidden="true" />
            {warningCount} {warningCount === 1 ? 'warning' : 'warnings'}
          </span>
        )}
        <span className="ml-auto truncate font-mono" title={scope.label}>
          {scope.label}
        </span>
      </div>
      <IssueList
        filterable
        issues={toIssueItems(issues)}
        onSelect={(item) => {
          if (item.file) void openWorkspaceFile(item.file, { source: true, line: item.line ?? undefined })
        }}
      />
    </div>
  )
}

function HelpPane() {
  return (
    <div className="px-3 py-3 text-xs text-muted-foreground">
      <p className="font-semibold text-foreground">Workspace guide</p>
      <p className="mt-2">Structure groups flowgroups by pipeline and lists project, pipeline and job configuration. Files gives you the project folder tree. Tables shows resolved datasets for the selected environment.</p>
      <p className="mt-2">Graph changes update the same document as Code. Select a node and press Enter to edit its fields. Save writes the whole file. Validation and generation use saved project files and the scope shown in the command bar.</p>
      <p className="mt-2">Project defaults can be overridden by pipeline configuration. Job configuration defines orchestration. Open Project configuration to inspect sections and the effective values for saved configuration.</p>
      <dl className="mt-3 grid grid-cols-[1fr_auto] gap-2">
        <dt>Quick open</dt><dd>Ctrl/⌘ K</dd>
        <dt>Save document</dt><dd>Ctrl/⌘ S</dd>
        <dt>Explorer</dt><dd>Ctrl/⌘ B</dd>
        <dt>Inspector</dt><dd>Ctrl/⌘ I</dd>
        <dt>Bottom panel</dt><dd>Ctrl/⌘ J</dd>
        <dt>Focus mode</dt><dd>Ctrl/⌘ Shift F</dd>
        <dt>Previous / next view</dt><dd>Alt ← / →</dd>
      </dl>
      <p className="mt-3">Right-click a tab for bulk close, pin and navigation actions. Unsaved documents are listed before a bulk close.</p>
    </div>
  )
}

/** Collapsed rail: icon buttons that expand the inspector onto their tab. */
function InspectorRail({
  onExpand,
  issueCount,
}: {
  onExpand: (tab: InspectorTab) => void
  issueCount: number
}) {
  return (
    <div className="flex h-full w-full flex-col items-center gap-1.5 border-l border-border bg-surface py-2">
      <button
        type="button"
        onClick={() => onExpand('validation')}
        aria-label="Expand inspector — Validation"
        title="Validation"
        className="relative flex size-7 items-center justify-center rounded-md text-muted-foreground transition-colors hover:bg-muted/60 hover:text-foreground"
      >
        <ClipboardCheck className="size-4" aria-hidden="true" />
        {issueCount > 0 && (
          <span className="absolute -top-0.5 -right-0.5 flex min-w-3.5 items-center justify-center rounded-full bg-error px-0.5 text-[9px] leading-none text-white tabular-nums">
            {issueCount > 99 ? '99+' : issueCount}
          </span>
        )}
      </button>
      <button
        type="button"
        onClick={() => onExpand('help')}
        aria-label="Expand inspector — Help"
        title="Help"
        className="flex size-7 items-center justify-center rounded-md text-muted-foreground transition-colors hover:bg-muted/60 hover:text-foreground"
      >
        <CircleHelp className="size-4" aria-hidden="true" />
      </button>
      <span className="mt-1 text-[10px] font-bold tracking-[0.12em] text-faint uppercase [writing-mode:vertical-rl]">
        Inspector
      </span>
    </div>
  )
}

export function Inspector() {
  const collapsed = useLayoutStore((s) => s.inspectorCollapsed)
  const setCollapsed = useLayoutStore((s) => s.setInspectorCollapsed)
  const tab = useLayoutStore((s) => s.inspectorTab)
  const setTab = useLayoutStore((s) => s.setInspectorTab)
  const issues = useRunStore((s) => s.issues)
  const activeTab = useActiveTab()

  // Config surfaces validate client-side (config-model); those issues live
  // outside runStore, so merge the active config tab's validator findings into
  // the run-scoped set (no runStore pollution — §6.3).
  const configIssues = useActiveConfigIssues(activeTab)
  const scoped = mergeConfigIssues(scopeIssues(issues, activeTab), configIssues, activeTab, issues)

  const expand = useCallback(
    (next: InspectorTab) => {
      setTab(next)
      setCollapsed(false)
    },
    [setTab, setCollapsed],
  )

  if (collapsed) {
    return <InspectorRail onExpand={expand} issueCount={scoped.issues.length} />
  }

  return (
    <div className="flex h-full w-full min-w-0 flex-col border-l border-border bg-surface">
      <div
        role="tablist" onKeyDown={tablistKeyDown}
        aria-label="Inspector"
        className="flex h-8 shrink-0 items-center border-b border-border pr-1 pl-1"
      >
        <TabButton
          active={tab === 'validation'}
          onClick={() => setTab('validation')}
          label="Validation"
          count={scoped.issues.length}
        />
        <TabButton active={tab === 'help'} onClick={() => setTab('help')} label="Help" />
        <button
          type="button"
          onClick={() => setCollapsed(true)}
          aria-label="Collapse inspector"
          title="Collapse"
          className="ml-auto flex size-6 items-center justify-center rounded-md text-muted-foreground transition-colors hover:bg-muted/60 hover:text-foreground"
        >
          <ChevronRight className="size-4" aria-hidden="true" />
        </button>
      </div>
      <div className="min-h-0 flex-1 overflow-auto">
        {/* Validation is the safe default: only 'help' renders HelpPane, so any
            unexpected value falls back to the Validation pane. */}
        {tab === 'help' ? <HelpPane /> : <ValidationPane scoped={scoped} />}
      </div>
    </div>
  )
}

function TabButton({
  active,
  onClick,
  label,
  count,
}: {
  active: boolean
  onClick: () => void
  label: string
  count?: number
}) {
  return (
    <button
      type="button"
      role="tab"
      aria-selected={active}
      tabIndex={active ? 0 : -1}
      onClick={onClick}
      className={cn(
        'flex h-8 items-center gap-1.5 border-b-2 px-2.5 text-xs font-medium transition-colors',
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
    </button>
  )
}
