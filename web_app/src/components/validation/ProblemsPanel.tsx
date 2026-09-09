import { CircleX, Loader2, TriangleAlert } from 'lucide-react'
import { useRunStore } from '../../store/runStore'
import { useWorkspaceStore } from '../../store/workspaceStore'
import { relativeTime } from '../../lib/utils'
import type { ValidationIssue } from '../../types/api'
import { IssueList } from './IssueList'
import { openWorkspaceFile } from '@/workspace/openWorkspaceFile'
import type { IssueListItem } from './IssueList'

// ── ProblemsPanel — compact post-run issues list ────────────
//
// A dense, clickable list of the issues from the most recent run,
// rendered through the shared `IssueList` rows (same presentational
// component as run history). Clicking an entry that carries a `file`
// opens that file as a workspace editor buffer (via the workspaceStore
// `openBuffer` opener).
//
// The file API is keyed on project-relative paths. The backend reports
// `file_path` as project-relative (the tree no longer carries a project
// root to strip), so we just normalize away any leading slash and open
// the path as-is.

/** Project stream `ValidationIssue`s onto the shared `IssueList` row shape. */
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

export function ProblemsPanel() {
  const issues = useRunStore((s) => s.issues)
  const isRunning = useRunStore((s) => s.isRunning)
  const hydratedFrom = useRunStore((s) => s.hydratedFrom)
  const dirty = useWorkspaceStore((s) => s.buffers.some((b) => b.isDirty))
  const errorCount = issues.filter((i) => i.severity === 'error').length
  const warningCount = issues.length - errorCount
  const countSummary = [
    errorCount > 0 ? `${errorCount} ${errorCount === 1 ? 'error' : 'errors'}` : null,
    warningCount > 0 ? `${warningCount} ${warningCount === 1 ? 'warning' : 'warnings'}` : null,
  ]
    .filter(Boolean)
    .join(', ')

  return (
    <>
      {/* Persistent live region: stays mounted across empty↔populated so
          screen readers reliably announce count changes (a region that
          mounts already holding content is not announced). */}
      <span role="status" className="sr-only">
        {issues.length > 0 ? `Problems: ${countSummary}` : ''}
      </span>
      {dirty && <p className="px-3 py-1 text-xs text-warning">Results describe saved files. Save and validate to include your current edits.</p>}
      {issues.length > 0 && (
        <div className="rounded-lg border border-border bg-card">
          <div className="flex items-center gap-3 border-b border-border px-3 py-2">
            <span className="text-sm font-semibold text-foreground">Problems</span>
            <span className="flex items-center gap-3 text-xs">
              {errorCount > 0 && (
                <span className="flex items-center gap-1 text-error">
                  <CircleX className="size-3.5" aria-hidden="true" />
                  {errorCount} {errorCount === 1 ? 'error' : 'errors'}
                </span>
              )}
              {warningCount > 0 && (
                <span className="flex items-center gap-1 text-warning">
                  <TriangleAlert className="size-3.5" aria-hidden="true" />
                  {warningCount} {warningCount === 1 ? 'warning' : 'warnings'}
                </span>
              )}
            </span>
            {isRunning && (
              <span className="flex items-center gap-1 text-xs text-muted-foreground">
                <Loader2 className="size-3 animate-spin" aria-hidden="true" />
                running…
              </span>
            )}
            {!isRunning && hydratedFrom && (
              <span className="ml-auto truncate text-2xs text-muted-foreground">
                from last validation
                {hydratedFrom.startedAt ? ` · ${relativeTime(hydratedFrom.startedAt)}` : ''}
                {hydratedFrom.env
                  ? ` · ${hydratedFrom.env}${hydratedFrom.pipeline ? `/${hydratedFrom.pipeline}` : ''}`
                  : ''}
              </span>
            )}
          </div>
          <IssueList
        filterable
            issues={toIssueItems(issues)}
            onSelect={(item) => {
              // Rows without a file are inert (nothing to open).
              if (item.file) void openWorkspaceFile(item.file, { source: true, line: item.line ?? undefined })
            }}
          />
        </div>
      )}
    </>
  )
}
