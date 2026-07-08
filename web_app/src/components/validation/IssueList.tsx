import { CircleX, Info, TriangleAlert } from 'lucide-react'
import { cn } from '@/lib/utils'

// ── IssueList — presentational issue rows ───────────────────
//
// A dense list of diagnostic rows: severity icon + mono code + message +
// `file:line` location. Purely presentational — no store access, no file
// opening — so pages with differently-shaped issue payloads (run history
// `RunIssue`, stream `ValidationIssue`) map onto `IssueListItem` and reuse
// the same rendering. When `onSelect` is provided, rows render as buttons.

export interface IssueListItem {
  /** 'error' | 'warning' | anything else (rendered as info). */
  severity: string
  code: string | null
  message: string
  /** Project-relative file path, when the issue points at a file. */
  file: string | null
  line: number | null
}

function SeverityIcon({ severity }: { severity: string }) {
  if (severity === 'error') {
    return <CircleX className="size-3.5 shrink-0 text-error" aria-hidden="true" />
  }
  if (severity === 'warning') {
    return <TriangleAlert className="size-3.5 shrink-0 text-warning" aria-hidden="true" />
  }
  return <Info className="size-3.5 shrink-0 text-info" aria-hidden="true" />
}

/** VS Code-style location suffix: `file.yaml` or `file.yaml:12`. */
function fileLocation(issue: IssueListItem): string | null {
  if (!issue.file) return null
  const name = issue.file.split('/').pop() ?? issue.file
  return issue.line != null ? `${name}:${issue.line}` : name
}

export function IssueList({
  issues,
  onSelect,
  className,
}: {
  issues: IssueListItem[]
  onSelect?: (issue: IssueListItem, index: number) => void
  className?: string
}) {
  if (issues.length === 0) return null

  return (
    <ul className={cn('divide-y divide-border/60', className)}>
      {issues.map((issue, i) => {
        const location = fileLocation(issue)
        const row = (
          <>
            <SeverityIcon severity={issue.severity} />
            <span className="font-mono text-2xs text-muted-foreground">
              {issue.code ?? ''}
            </span>
            <span className="truncate text-xs text-foreground">{issue.message}</span>
            {location ? (
              <span
                title={issue.file ?? undefined}
                className="font-mono text-2xs text-muted-foreground"
              >
                {location}
              </span>
            ) : (
              <span />
            )}
          </>
        )
        const rowClass =
          'grid min-h-7 w-full grid-cols-[16px_max-content_minmax(0,1fr)_max-content] items-center gap-2 px-3 py-0.5 text-left'
        return (
          <li key={`${issue.code ?? 'issue'}-${i}`}>
            {onSelect ? (
              <button
                type="button"
                onClick={() => onSelect(issue, i)}
                title={issue.message}
                className={cn(rowClass, 'hover:bg-muted/50')}
              >
                {row}
              </button>
            ) : (
              <div title={issue.message} className={rowClass}>
                {row}
              </div>
            )}
          </li>
        )
      })}
    </ul>
  )
}
