import { useState } from 'react'
import {
  ChevronDown,
  ChevronRight,
  CircleCheck,
  CircleX,
  History,
  Loader2,
  Play,
} from 'lucide-react'
import { useRun, useRuns } from '../../../hooks/useRuns'
import { EmptyState } from '../../common/EmptyState'
import { TableSkeleton } from '../../common/SkeletonLoader'
import { IssueList } from '../../validation/IssueList'
import type { IssueListItem } from '../../validation/IssueList'
import { JsonTree } from '../../detail/JsonTree'
import { errorMessage } from '../../../lib/errors'
import type { RunIssue, RunSummary } from '../../../types/api'
import { Badge } from '../../ui/badge'
import { Button } from '../../ui/button'
import { cn } from '../../../lib/utils'

// ── RunHistoryView — the bottom-panel History tab (§6.7 / D12) ──
//
// Recomposes the RunHistoryPage table (useRuns/useRun expandable rows) into
// the bottom dock, dropping the page's title/description/padding chrome. The
// original pages/RunHistoryPage.tsx stays untouched on disk (it is deleted in
// T1.6). Presentational helpers are copied verbatim; the data hooks, IssueList
// and JsonTree are reused directly.

function StatusBadge({ status }: { status: string }) {
  if (status === 'completed') {
    return (
      <Badge className="h-5 rounded-sm border border-success/25 bg-success/12 px-1.5 text-2xs text-success">
        <CircleCheck className="size-2.5" aria-hidden="true" />
        completed
      </Badge>
    )
  }
  if (status === 'failed') {
    return (
      <Badge className="h-5 rounded-sm border border-error/25 bg-error/12 px-1.5 text-2xs text-error">
        <CircleX className="size-2.5" aria-hidden="true" />
        failed
      </Badge>
    )
  }
  if (status === 'running') {
    return (
      <Badge className="h-5 rounded-sm border border-info/25 bg-info/12 px-1.5 text-2xs text-info">
        <Loader2 className="size-2.5 animate-spin" aria-hidden="true" />
        running
      </Badge>
    )
  }
  return (
    <Badge variant="outline" className="h-5 rounded-sm px-1.5 text-2xs text-muted-foreground">
      {status}
    </Badge>
  )
}

function KindBadge({ kind }: { kind: string }) {
  const Icon = kind === 'generate' ? Play : CircleCheck
  return (
    <Badge variant="outline" className="h-5 rounded-sm px-1.5 text-2xs text-muted-foreground">
      <Icon className="size-2.5" aria-hidden="true" />
      {kind}
    </Badge>
  )
}

function formatStarted(iso: string): string {
  const date = new Date(iso)
  return Number.isNaN(date.getTime()) ? iso : date.toLocaleString()
}

function formatDuration(run: RunSummary): string {
  if (!run.finished_at) return '—'
  const start = new Date(run.started_at).getTime()
  const finish = new Date(run.finished_at).getTime()
  if (Number.isNaN(start) || Number.isNaN(finish)) return '—'
  const seconds = (finish - start) / 1000
  return seconds >= 60
    ? `${Math.floor(seconds / 60)}m ${Math.round(seconds % 60)}s`
    : `${seconds.toFixed(1)}s`
}

function toIssueItems(issues: RunIssue[]): IssueListItem[] {
  return issues.map((issue) => ({
    severity: issue.severity,
    code: issue.code,
    message: issue.message,
    file: issue.file,
    line: issue.line,
  }))
}

function RunDetailPanel({ runId }: { runId: string }) {
  const [showEvents, setShowEvents] = useState(false)
  const { data, isLoading, isError, error } = useRun(runId, showEvents)

  if (isLoading) return <TableSkeleton rows={3} />
  if (isError) {
    return (
      <p className="px-3 py-2 text-xs text-error">
        {errorMessage(error, `Could not load run '${runId}'.`)}
      </p>
    )
  }
  if (!data) return null

  const events = data.events ?? []

  return (
    <div className="space-y-3 px-3 py-2.5">
      {data.summary && Object.keys(data.summary).length > 0 && (
        <div>
          <h4 className="mb-1.5 text-2xs font-semibold tracking-[0.05em] text-muted-foreground uppercase">
            Summary
          </h4>
          <JsonTree data={data.summary} />
        </div>
      )}

      <div>
        <h4 className="mb-1.5 text-2xs font-semibold tracking-[0.05em] text-muted-foreground uppercase">
          Issues ({data.issues.length})
        </h4>
        {data.issues.length > 0 ? (
          <div className="rounded-md border border-border">
            <IssueList issues={toIssueItems(data.issues)} />
          </div>
        ) : (
          <p className="text-xs text-muted-foreground">No issues recorded for this run.</p>
        )}
      </div>

      <div>
        <Button
          variant="outline"
          size="xs"
          onClick={() => setShowEvents((s) => !s)}
          aria-expanded={showEvents}
        >
          {showEvents ? <ChevronDown /> : <ChevronRight />}
          {showEvents ? 'Hide events' : 'Show events'}
        </Button>
        {showEvents &&
          (events.length > 0 ? (
            <div className="mt-2 max-h-64 overflow-y-auto rounded-md border border-border">
              <ul className="divide-y divide-border/60">
                {events.map((frame, i) => (
                  <li key={i} className="px-2 py-1">
                    <JsonTree data={frame} />
                  </li>
                ))}
              </ul>
            </div>
          ) : (
            <p className="mt-2 text-xs text-muted-foreground">
              No event frames recorded for this run.
            </p>
          ))}
      </div>
    </div>
  )
}

function RunRow({
  run,
  selected,
  onToggle,
}: {
  run: RunSummary
  selected: boolean
  onToggle: () => void
}) {
  return (
    <>
      <tr
        onClick={onToggle}
        className={cn(
          'cursor-pointer transition-colors hover:bg-muted/50',
          selected && 'bg-muted/50',
        )}
      >
        <td className="px-1.5 py-1.5">
          <Button
            variant="ghost"
            size="icon-xs"
            aria-expanded={selected}
            aria-label={selected ? 'Collapse run details' : 'Expand run details'}
            onClick={(e) => {
              e.stopPropagation()
              onToggle()
            }}
            className="text-muted-foreground"
          >
            {selected ? <ChevronDown /> : <ChevronRight />}
          </Button>
        </td>
        <td className="px-3 py-1.5">
          <KindBadge kind={run.kind} />
        </td>
        <td className="px-3 py-1.5 font-mono text-foreground">{run.env}</td>
        <td className="px-3 py-1.5 font-mono text-muted-foreground">
          {run.pipeline ?? 'all pipelines'}
        </td>
        <td className="px-3 py-1.5">
          <StatusBadge status={run.status} />
        </td>
        <td className="px-3 py-1.5 whitespace-nowrap text-muted-foreground tabular-nums">
          {formatStarted(run.started_at)}
        </td>
        <td className="px-3 py-1.5 whitespace-nowrap text-muted-foreground tabular-nums">
          {formatDuration(run)}
        </td>
      </tr>
      {selected && (
        <tr>
          <td colSpan={7} className="border-t border-border/60 bg-background/50 p-0">
            <RunDetailPanel runId={run.run_id} />
          </td>
        </tr>
      )}
    </>
  )
}

export function RunHistoryView() {
  const { data, isLoading, isError, error } = useRuns(50)
  const [selectedRunId, setSelectedRunId] = useState<string | null>(null)

  const runs = data?.runs ?? []

  if (isLoading) {
    return (
      <div className="p-3">
        <TableSkeleton rows={6} />
      </div>
    )
  }

  if (isError) {
    return (
      <EmptyState
        title="Failed to load run history"
        message={errorMessage(error, 'The runs endpoint is unavailable.')}
        icon={History}
      />
    )
  }

  if (runs.length === 0) {
    return (
      <EmptyState
        title="No runs yet"
        message="Validate or generate a pipeline and its run will be recorded here."
        icon={History}
      />
    )
  }

  return (
    <div className="h-full overflow-auto">
      <table className="w-full text-xs">
        <thead>
          <tr className="border-b border-border bg-muted/50 text-left text-2xs tracking-[0.05em] text-muted-foreground uppercase">
            <th className="w-8 px-3 py-2" aria-label="Expand" />
            <th className="px-3 py-2 font-semibold">Kind</th>
            <th className="px-3 py-2 font-semibold">Env</th>
            <th className="px-3 py-2 font-semibold">Pipeline</th>
            <th className="px-3 py-2 font-semibold">Status</th>
            <th className="px-3 py-2 font-semibold">Started</th>
            <th className="px-3 py-2 font-semibold">Duration</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-border/60">
          {runs.map((run) => {
            const selected = run.run_id === selectedRunId
            return (
              <RunRow
                key={run.run_id}
                run={run}
                selected={selected}
                onToggle={() => setSelectedRunId(selected ? null : run.run_id)}
              />
            )
          })}
        </tbody>
      </table>
    </div>
  )
}
