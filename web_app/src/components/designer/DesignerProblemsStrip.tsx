import { useState } from 'react'
import { ChevronDown, ChevronUp, CircleCheck, CircleX, TriangleAlert } from 'lucide-react'
import { cn } from '@/lib/utils'
import type { MappedIssue } from './designerValidation'
import type { ValidateStatus } from './useDesignerValidation'

// ── DesignerProblemsStrip — post-run issues for the flowgroup ─
//
// A collapsible strip docked at the bottom of the designer, shown only after
// an on-demand validate completes. It lists every issue for the flowgroup
// with severity, code, title, details, and suggestions; a row whose issue
// mapped to an action selects that node on click (unmappable rows are
// flowgroup-level and inert). The severity vocabulary (CircleX/error,
// TriangleAlert/warning) matches the per-node badges and the Validate button.

interface DesignerProblemsStripProps {
  status: ValidateStatus
  errorCount: number
  warningCount: number
  issues: MappedIssue[]
  errored: boolean
  onSelectNode: (nodeId: string) => void
}

function CountPill({
  icon: Icon,
  count,
  className,
}: {
  icon: typeof CircleX
  count: number
  className: string
}) {
  return (
    <span className={cn('flex items-center gap-1', className)}>
      <Icon className="size-3.5" aria-hidden="true" />
      {count}
    </span>
  )
}

export function DesignerProblemsStrip({
  status,
  errorCount,
  warningCount,
  issues,
  errored,
  onSelectNode,
}: DesignerProblemsStripProps) {
  const [open, setOpen] = useState(true)

  if (status !== 'done') return null

  const total = issues.length

  if (total === 0) {
    return (
      <div className="flex shrink-0 items-center gap-2 border-t border-border bg-card px-4 py-1.5 text-xs">
        {errored ? (
          <>
            <CircleX className="size-3.5 shrink-0 text-error" aria-hidden="true" />
            <span className="text-foreground">Validation couldn&apos;t complete — see the run panel.</span>
          </>
        ) : (
          <>
            <CircleCheck className="size-3.5 shrink-0 text-success" aria-hidden="true" />
            <span className="text-foreground">No problems found.</span>
          </>
        )}
      </div>
    )
  }

  return (
    <div className="flex max-h-56 shrink-0 flex-col border-t border-border bg-card">
      <button
        type="button"
        onClick={() => setOpen((v) => !v)}
        aria-expanded={open}
        className="flex shrink-0 items-center gap-3 px-4 py-1.5 text-left hover:bg-muted/50"
      >
        <span className="text-2xs font-semibold uppercase tracking-[0.05em] text-muted-foreground">
          Problems
        </span>
        <span className="flex items-center gap-3 text-xs">
          {errorCount > 0 && <CountPill icon={CircleX} count={errorCount} className="text-error" />}
          {warningCount > 0 && (
            <CountPill icon={TriangleAlert} count={warningCount} className="text-warning" />
          )}
        </span>
        <span className="flex-1" />
        {open ? (
          <ChevronDown className="size-3.5 text-muted-foreground" aria-hidden="true" />
        ) : (
          <ChevronUp className="size-3.5 text-muted-foreground" aria-hidden="true" />
        )}
      </button>

      {open && (
        <ul className="min-h-0 flex-1 divide-y divide-border overflow-y-auto border-t border-border">
          {issues.map(({ issue, nodeId }, i) => {
            const Icon = issue.severity === 'error' ? CircleX : TriangleAlert
            const tone = issue.severity === 'error' ? 'text-error' : 'text-warning'
            const selectable = nodeId !== null
            return (
              <li key={`${issue.code}-${i}`}>
                <button
                  type="button"
                  disabled={!selectable}
                  onClick={selectable ? () => onSelectNode(nodeId) : undefined}
                  className={cn(
                    'flex w-full items-start gap-2 px-4 py-2 text-left',
                    selectable ? 'cursor-pointer hover:bg-muted/50' : 'cursor-default',
                  )}
                >
                  <Icon className={cn('mt-0.5 size-3.5 shrink-0', tone)} aria-hidden="true" />
                  <div className="min-w-0 flex-1">
                    <div className="flex items-center gap-2">
                      <span className="font-mono text-2xs text-muted-foreground">{issue.code}</span>
                      {!selectable && (
                        <span className="rounded-sm border border-border px-1 text-2xs text-muted-foreground">
                          flowgroup
                        </span>
                      )}
                    </div>
                    <div className="break-words text-xs text-foreground">{issue.title}</div>
                    {issue.details !== null && issue.details !== '' && (
                      <p className="mt-0.5 break-words text-2xs text-muted-foreground">
                        {issue.details}
                      </p>
                    )}
                    {issue.suggestions.map((s, si) => (
                      <p key={si} className="mt-0.5 break-words text-2xs text-muted-foreground">
                        → {s}
                      </p>
                    ))}
                  </div>
                </button>
              </li>
            )
          })}
        </ul>
      )}
    </div>
  )
}
