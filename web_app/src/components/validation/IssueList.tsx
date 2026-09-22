import { useState } from 'react'
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
  details?: string | null
  suggestions?: string[]
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
  filterable = false,
}: {
  issues: IssueListItem[]
  onSelect?: (issue: IssueListItem, index: number) => void
  className?: string
  filterable?: boolean
}) {
  const [severity, setSeverity] = useState('')
  const [file, setFile] = useState('')
  if (issues.length === 0) return null
  const shown = issues.map((issue, index) => ({ issue, index })).filter(({ issue }) =>
    !filterable || ((!severity || issue.severity === severity)
      && (!file.trim() || (issue.file ?? '').toLowerCase().includes(file.trim().toLowerCase()))))

  return (
    <>
      {filterable && (
        <div className="flex flex-wrap items-center gap-2 border-b border-border px-3 py-2 text-xs">
          <select aria-label="Issue severity" className="rounded border border-border bg-background px-2 py-1" value={severity} onChange={(event) => setSeverity(event.target.value)}>
            <option value="">All severities</option>
            <option value="error">Errors</option>
            <option value="warning">Warnings</option>
          </select>
          <input type="search" aria-label="Filter issues by file" placeholder="Filter by file path…" className="min-w-24 flex-1 rounded border border-border bg-background px-2 py-1" value={file} onChange={(event) => setFile(event.target.value)} />
          {(severity || file) && <button type="button" className="rounded px-1 py-1 text-primary hover:bg-accent-weak" onClick={() => { setSeverity(''); setFile('') }}>Clear filters</button>}
          <span className="text-muted-foreground" aria-live="polite">{shown.length} of {issues.length}</span>
        </div>
      )}
      {shown.length === 0 && <p className="px-3 py-2 text-xs text-muted-foreground">No issues match these filters.</p>}
    <ul className={cn('divide-y divide-border/60', className)}>
      {shown.map(({ issue, index: i }) => {
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
            <details className="px-3 pb-1 text-xs text-muted-foreground">
              <summary className="cursor-pointer py-1 focus-visible:outline-ring">Details{issue.file ? ` · ${issue.file}` : ''}</summary>
              <p className="whitespace-pre-wrap break-words py-1 text-foreground">{issue.message}</p>
              {issue.details && <p className="whitespace-pre-wrap break-words py-1">{issue.details}</p>}
              {!!issue.suggestions?.length && <ul className="list-disc space-y-1 pl-4">{issue.suggestions.map((suggestion, index) => <li key={index}>{suggestion}</li>)}</ul>}
            </details>
          </li>
        )
      })}
    </ul>
    </>
  )
}
