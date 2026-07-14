import type { ValidationIssue } from '../../../types/api'
import type { WorkspaceTabRef } from '../../../store/workspaceStore'

// ── scopeIssues — narrow the run issue set to the active center tab ──
//
// The inspector's Validation tab shows issues scoped to whatever entity is
// active in the center (§3 / §6.4). Pure and unit-tested so the Inspector
// component stays a thin renderer:
//   • entity tab   → issues for that pipeline + flowgroup
//   • file tab     → issues whose file_path is that file (project-wide when
//                    the file has no file-scoped issues)
//   • config/resource tab → same file-path match
//   • project-map / table-detail / no tab → the whole project
//
// `projectWide` tells the caller whether the shown set is narrowed, so the
// status line can label the scope.

export interface IssueScope {
  /** Status-line label: 'project', a flowgroup name, or a filename. */
  label: string
  /** True when the shown set spans the whole project (not narrowed). */
  projectWide: boolean
}

export interface ScopedIssues {
  issues: ValidationIssue[]
  scope: IssueScope
}

const PROJECT: IssueScope = { label: 'project', projectWide: true }

/** Strip a leading slash so reported `file_path`s compare against the
 * project-relative paths tabs carry. */
function toProjectRelative(path: string): string {
  return path.replace(/^\/+/, '')
}

function basename(path: string): string {
  return path.split('/').pop() || path
}

function scopeByFile(issues: readonly ValidationIssue[], path: string): ScopedIssues {
  const rel = toProjectRelative(path)
  const matched = issues.filter(
    (i) => i.file_path !== null && toProjectRelative(i.file_path) === rel,
  )
  // No file-scoped issues for this file → fall back to the whole project so
  // the tab is never confusingly empty (per T1.5 scoping rule).
  if (matched.length === 0) return { issues: [...issues], scope: PROJECT }
  return { issues: matched, scope: { label: basename(rel), projectWide: false } }
}

/** Project the run's issues down to the ones relevant to `activeTab`. */
export function scopeIssues(
  issues: readonly ValidationIssue[],
  activeTab: WorkspaceTabRef | null,
): ScopedIssues {
  if (activeTab === null) return { issues: [...issues], scope: PROJECT }

  switch (activeTab.kind) {
    case 'entity':
    case 'designer': {
      // 'designer' is the retired kind still present during the Wave-1
      // relocation; it carries the same pipeline/flowgroup identity.
      const matched = issues.filter(
        (i) =>
          i.pipeline_name === activeTab.pipeline &&
          i.flowgroup_name === activeTab.flowgroup,
      )
      return {
        issues: matched,
        scope: { label: activeTab.flowgroup || 'entity', projectWide: false },
      }
    }
    case 'file':
      return scopeByFile(issues, activeTab.path)
    case 'config':
      return scopeByFile(issues, activeTab.path)
    case 'resource':
      return scopeByFile(issues, activeTab.filePath)
    case 'project-map':
    case 'pipeline-dag':
    case 'table-detail':
      return { issues: [...issues], scope: PROJECT }
  }
}
