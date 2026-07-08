import { CircleX, TriangleAlert } from 'lucide-react'
import type { LucideIcon } from 'lucide-react'
import type { ValidationIssue } from '../../types/api'
import { cn } from '@/lib/utils'

// ── ValidationResults — structured ValidationIssue[] renderer ──
//
// Replaces the legacy flat `string[]` errors/warnings rendering. Issues
// are grouped by severity (errors first), and each card surfaces the
// code, title, details, originating file, and any suggestions.

function severityStyles(severity: ValidationIssue['severity']): {
  card: string
  heading: string
  badge: string
  icon: LucideIcon
  iconClassName: string
} {
  if (severity === 'error') {
    return {
      card: 'border-error/25 bg-error/12',
      heading: 'text-error',
      badge: 'border-error/25 bg-error/12 text-error',
      icon: CircleX,
      iconClassName: 'text-error',
    }
  }
  return {
    card: 'border-warning/25 bg-warning/12',
    heading: 'text-warning',
    badge: 'border-warning/25 bg-warning/12 text-warning',
    icon: TriangleAlert,
    iconClassName: 'text-warning',
  }
}

function IssueCard({ issue }: { issue: ValidationIssue }) {
  const styles = severityStyles(issue.severity)
  const Icon = styles.icon
  const location = [issue.pipeline_name, issue.flowgroup_name]
    .filter(Boolean)
    .join(' › ')

  return (
    <div className={cn('rounded-lg border px-3 py-2 text-xs', styles.card)}>
      <div className="flex items-start gap-2">
        <Icon
          className={cn('mt-0.5 size-3.5 shrink-0', styles.iconClassName)}
          aria-hidden="true"
        />
        <span
          className={cn(
            'mt-px rounded-sm border px-1.5 font-mono text-2xs whitespace-nowrap',
            styles.badge,
          )}
        >
          {issue.code}
        </span>
        <div className="min-w-0 flex-1">
          <div className="font-medium text-foreground">{issue.title}</div>
          {issue.details && (
            <div className="mt-0.5 whitespace-pre-wrap text-muted-foreground">
              {issue.details}
            </div>
          )}
          {location && (
            <div className="mt-0.5 text-2xs text-muted-foreground">{location}</div>
          )}
          {issue.file_path && (
            <div className="mt-0.5 font-mono text-2xs text-muted-foreground">
              {issue.file_path}
            </div>
          )}
          {issue.suggestions.length > 0 && (
            <ul className="mt-1 list-inside list-disc space-y-0.5 text-2xs text-muted-foreground">
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
              className="mt-1 inline-block text-2xs text-primary hover:underline"
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
  const Icon = styles.icon
  return (
    <div>
      <h3
        className={cn(
          'mb-2 flex items-center gap-1.5 text-xs font-semibold',
          styles.heading,
        )}
      >
        <Icon className="size-3.5" aria-hidden="true" />
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
