import { useState } from 'react'
import { useHistoryViewState } from '../../../store/runHistoryViewStore'
import { toast } from 'sonner'
import { fetchRun } from '../../../api/runs'
import { openWorkspaceFile } from '../../../workspace/openWorkspaceFile'
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogDescription } from '../../ui/dialog'
import {
  Download,
  Maximize2,
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
  const showEvents = useHistoryViewState((s) => s.eventsFor[runId] ?? false)
  const setShowEvents = () => useHistoryViewState.setState((s) => ({ eventsFor: { ...s.eventsFor, [runId]: !showEvents } }))
  const [exporting, setExporting] = useState(false)
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
  const exportRun = async () => {
    setExporting(true)
    try {
      const complete = await fetchRun(runId, true)
      const url = URL.createObjectURL(new Blob([JSON.stringify(complete, null, 2)], { type: 'application/json' }))
      const anchor = document.createElement('a')
      anchor.href = url
      anchor.download = `lhp-run-${runId}.json`
      anchor.click()
      setTimeout(() => URL.revokeObjectURL(url), 0)
    } catch (error) { toast.error(errorMessage(error, 'Could not export run')) }
    finally { setExporting(false) }
  }

  return (
    <div className="space-y-3 px-3 py-2.5">
      <div className="flex flex-wrap items-center justify-between gap-2">
        <p className="text-xs text-muted-foreground">Recorded results for saved files at run time. Current edits may differ.</p>
        <Button variant="outline" size="xs" disabled={exporting} onClick={() => { void exportRun() }}><Download /> Export run JSON</Button>
      </div>
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
            <IssueList issues={toIssueItems(data.issues)} onSelect={(issue) => {
              if (issue.file) void openWorkspaceFile(issue.file, { source: true, line: issue.line ?? undefined })
            }} />
          </div>
        ) : (
          <p className="text-xs text-muted-foreground">No issues recorded for this run.</p>
        )}
      </div>

      <div>
        <Button
          variant="outline"
          size="xs"
          onClick={setShowEvents}
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
  onExpand,
}: {
  run: RunSummary
  selected: boolean
  onToggle: () => void
  onExpand: () => void
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
          <time dateTime={run.started_at} title={run.started_at}>{formatStarted(run.started_at)}</time>
        </td>
        <td className="px-3 py-1.5 whitespace-nowrap text-muted-foreground tabular-nums">
          {formatDuration(run)}
        </td>
      </tr>
      {selected && (
        <tr>
          <td colSpan={7} className="border-t border-border/60 bg-background/50 p-0">
            <div className="flex justify-end px-3 pt-2"><Button variant="ghost" size="xs" onClick={onExpand}><Maximize2 /> Expand run details</Button></div>
            <RunDetailPanel runId={run.run_id} />
          </td>
        </tr>
      )}
    </>
  )
}

export function RunHistoryView() {
  const { selectedRunId, env, pipeline, status, limit } = useHistoryViewState()
  const { data, isLoading, isError, error, isFetching, refetch } = useRuns(limit)
  const [expandedRunId, setExpandedRunId] = useState<string | null>(null)
  const allRuns = data?.runs ?? []
  const runs = allRuns.filter((run) => (!env || run.env === env)
    && (!pipeline || (run.pipeline ?? '__all__') === pipeline) && (!status || run.status === status))
  const environments = [...new Set(allRuns.map((run) => run.env).filter(Boolean))].sort()
  const pipelines = [...new Set(allRuns.map((run) => run.pipeline ?? '__all__'))].sort()

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
        action={{ label: 'Retry', onClick: () => { void refetch() } }}
      />
    )
  }

  if (allRuns.length === 0) {
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
      <div className="sticky top-0 z-10 flex flex-wrap items-center gap-2 border-b border-border bg-surface px-3 py-2 text-xs">
        <label className="flex items-center gap-1">Environment
          <select className="rounded border border-border bg-background px-2 py-1" value={env} onChange={(event) => useHistoryViewState.setState({ env: event.target.value })}>
            <option value="">All environments</option>{environments.map((name) => <option key={name} value={name}>{name}</option>)}
          </select>
        </label>
        <label className="flex items-center gap-1">Pipeline
          <select className="rounded border border-border bg-background px-2 py-1" value={pipeline} onChange={(event) => useHistoryViewState.setState({ pipeline: event.target.value })}>
            <option value="">All scopes</option>{pipelines.map((name) => <option key={name} value={name}>{name === '__all__' ? 'All pipelines run' : name}</option>)}
          </select>
        </label>
        <label className="flex items-center gap-1">Status
          <select className="rounded border border-border bg-background px-2 py-1" value={status} onChange={(event) => useHistoryViewState.setState({ status: event.target.value })}>
            <option value="">All statuses</option>{['running', 'completed', 'failed'].map((name) => <option key={name} value={name}>{name}</option>)}
          </select>
        </label>
        {(env || pipeline || status) && <Button variant="ghost" size="xs" onClick={() => useHistoryViewState.setState({ env: '', pipeline: '', status: '' })}>Clear filters</Button>}
        <span className="ml-auto text-muted-foreground">{runs.length} of {allRuns.length} loaded · local time ({Intl.DateTimeFormat().resolvedOptions().timeZone})</span>
      </div>
      {runs.length === 0 && <p className="p-3 text-xs text-muted-foreground">No loaded runs match these filters. Clear filters or load older runs.</p>}
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
                onToggle={() => useHistoryViewState.setState({ selectedRunId: selected ? null : run.run_id })}
                onExpand={() => setExpandedRunId(run.run_id)}
              />
            )
          })}
        </tbody>
      </table>
      <div className="flex items-center justify-center gap-2 border-t border-border p-2 text-xs text-muted-foreground">
        {allRuns.length >= limit && limit < 200 ? <Button variant="outline" size="sm" disabled={isFetching} onClick={() => useHistoryViewState.setState({ limit: Math.min(200, limit + 50) })}>{isFetching ? 'Loading…' : 'Load older runs'}</Button>
          : <span>{limit >= 200 && allRuns.length >= 200 ? 'Showing the latest 200 runs (server limit).' : 'All available runs loaded.'}</span>}
      </div>
      <Dialog open={expandedRunId !== null} onOpenChange={(open) => { if (!open) setExpandedRunId(null) }}>
        <DialogContent className="max-h-[90vh] overflow-auto sm:max-w-5xl">
          <DialogHeader><DialogTitle>Run details</DialogTitle><DialogDescription>{expandedRunId}</DialogDescription></DialogHeader>
          {expandedRunId && <RunDetailPanel runId={expandedRunId} />}
        </DialogContent>
      </Dialog>
    </div>
  )
}
