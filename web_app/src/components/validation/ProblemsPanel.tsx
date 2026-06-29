import { useCallback } from 'react'
import { useRunStore } from '../../store/runStore'
import { useUIStore } from '../../store/uiStore'
import { fetchFileContent } from '../../api/files'
import type { ValidationIssue } from '../../types/api'

// ── ProblemsPanel — compact post-run issues list ────────────
//
// A dense, clickable list of the issues from the most recent run.
// Clicking an entry that carries a `file_path` opens that file in the
// file-editor modal (via the uiStore `openFilePath` opener).
//
// The file API is keyed on project-relative paths. The backend reports
// `file_path` as project-relative (the tree no longer carries a project
// root to strip), so we just normalize away any leading slash and open
// the path as-is.

/** Normalize a reported `file_path` to a project-relative path suitable for
 * the file API (strip any leading slash). */
function toProjectRelative(filePath: string): string {
  return filePath.replace(/^\/+/, '')
}

function severityDot(severity: ValidationIssue['severity']): string {
  return severity === 'error' ? 'bg-red-500' : 'bg-amber-500'
}

export function ProblemsPanel() {
  const issues = useRunStore((s) => s.issues)
  const isRunning = useRunStore((s) => s.isRunning)
  const openFilePath = useUIStore((s) => s.openFilePath)

  const handleOpen = useCallback(
    async (filePath: string) => {
      const relative = toProjectRelative(filePath)
      if (relative === '') return
      try {
        const content = await fetchFileContent(relative)
        openFilePath(relative, content)
      } catch {
        // File missing / not readable — silently skip.
      }
    },
    [openFilePath],
  )

  if (issues.length === 0) return null

  const errorCount = issues.filter((i) => i.severity === 'error').length
  const warningCount = issues.length - errorCount

  return (
    <div className="rounded border border-slate-200 bg-white">
      <div className="flex items-center gap-3 border-b border-slate-200 px-3 py-2 text-xs font-semibold text-slate-600">
        <span>Problems</span>
        {errorCount > 0 && <span className="text-red-600">{errorCount} errors</span>}
        {warningCount > 0 && <span className="text-amber-600">{warningCount} warnings</span>}
        {isRunning && <span className="text-slate-400">(running…)</span>}
      </div>
      <ul className="divide-y divide-slate-100">
        {issues.map((issue, i) => {
          const clickable = !!issue.file_path
          return (
            <li key={`${issue.code}-${i}`}>
              <button
                type="button"
                disabled={!clickable}
                onClick={clickable ? () => void handleOpen(issue.file_path!) : undefined}
                className={`flex w-full items-start gap-2 px-3 py-1.5 text-left text-xs ${
                  clickable ? 'hover:bg-slate-50' : 'cursor-default'
                }`}
              >
                <span className={`mt-1 h-1.5 w-1.5 shrink-0 rounded-full ${severityDot(issue.severity)}`} />
                <span className="min-w-0 flex-1">
                  <span className="font-mono text-[10px] text-slate-400">{issue.code}</span>{' '}
                  <span className="text-slate-700">{issue.title}</span>
                  {issue.file_path && (
                    <span className="ml-1 font-mono text-[10px] text-blue-600">
                      {issue.file_path.split('/').pop()}
                    </span>
                  )}
                </span>
              </button>
            </li>
          )
        })}
      </ul>
    </div>
  )
}
