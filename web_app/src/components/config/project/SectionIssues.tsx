import type { ValidationIssue } from '../../../lib/config-model'

// Section-scope issue list (issues addressed to the section key itself,
// e.g. cross-field rules like "event_log is enabled but missing catalog").
// Field-scope issues render inline under their field instead.

export function SectionIssues({ issues }: { issues: ValidationIssue[] }) {
  if (issues.length === 0) return null
  return (
    <div className="space-y-1">
      {issues.map((issue, i) => (
        <p
          key={i}
          role="alert"
          className={
            issue.severity === 'error' ? 'text-2xs text-destructive' : 'text-2xs text-warning'
          }
        >
          {issue.message}
        </p>
      ))}
    </div>
  )
}
