import type { ValidationIssue } from '../../types/api'

// ── ValidationResults — structured ValidationIssue[] renderer ──
//
// Replaces the legacy flat `string[]` errors/warnings rendering. Issues
// are grouped by severity (errors first), and each card surfaces the
// code, title, details, originating file, and any suggestions.

function severityStyles(severity: ValidationIssue['severity']): {
  card: string
  heading: string
  badge: string
} {
  if (severity === 'error') {
    return {
      card: 'border-red-200 bg-red-50',
      heading: 'text-red-700',
      badge: 'bg-red-100 text-red-700',
    }
  }
  return {
    card: 'border-amber-200 bg-amber-50',
    heading: 'text-amber-700',
    badge: 'bg-amber-100 text-amber-700',
  }
}

function IssueCard({ issue }: { issue: ValidationIssue }) {
  const styles = severityStyles(issue.severity)
  const location = [issue.pipeline_name, issue.flowgroup_name]
    .filter(Boolean)
    .join(' › ')

  return (
    <div className={`rounded border px-3 py-2 text-xs ${styles.card}`}>
      <div className="flex items-start gap-2">
        <span className={`mt-0.5 rounded px-1.5 py-0.5 font-mono text-[10px] ${styles.badge}`}>
          {issue.code}
        </span>
        <div className="min-w-0 flex-1">
          <div className="font-medium text-slate-800">{issue.title}</div>
          {issue.details && (
            <div className="mt-0.5 whitespace-pre-wrap text-slate-600">{issue.details}</div>
          )}
          {location && (
            <div className="mt-0.5 text-[11px] text-slate-500">{location}</div>
          )}
          {issue.file_path && (
            <div className="mt-0.5 font-mono text-[11px] text-slate-500">{issue.file_path}</div>
          )}
          {issue.suggestions.length > 0 && (
            <ul className="mt-1 list-inside list-disc space-y-0.5 text-[11px] text-slate-600">
              {issue.suggestions.map((s, i) => (
                <li key={i}>{s}</li>
              ))}
            </ul>
          )}
          {issue.doc_link && (
            <a
              href={issue.doc_link}
              target="_blank"
              rel="noreferrer"
              className="mt-1 inline-block text-[11px] text-blue-600 hover:underline"
            >
              Documentation →
            </a>
          )}
        </div>
      </div>
    </div>
  )
}

function IssueGroup({
  title,
  issues,
}: {
  title: string
  issues: ValidationIssue[]
}) {
  if (issues.length === 0) return null
  const styles = severityStyles(issues[0].severity)
  return (
    <div>
      <h3 className={`mb-2 text-xs font-semibold ${styles.heading}`}>
        {title} ({issues.length})
      </h3>
      <div className="space-y-1.5">
        {issues.map((issue, i) => (
          <IssueCard key={`${issue.code}-${i}`} issue={issue} />
        ))}
      </div>
    </div>
  )
}

export function ValidationResults({ issues }: { issues: ValidationIssue[] }) {
  const errors = issues.filter((i) => i.severity === 'error')
  const warnings = issues.filter((i) => i.severity === 'warning')

  if (issues.length === 0) return null

  return (
    <div className="space-y-4">
      <IssueGroup title="Errors" issues={errors} />
      <IssueGroup title="Warnings" issues={warnings} />
    </div>
  )
}
