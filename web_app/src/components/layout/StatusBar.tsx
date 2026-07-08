import { NavLink } from 'react-router-dom'
import { CircleCheck, CircleX, Loader2, TriangleAlert, Wifi, WifiOff } from 'lucide-react'
import { Badge } from '../ui/badge'
import { cn } from '../../lib/utils'
import { useHealth } from '../../hooks/useProject'
import { useUIStore } from '../../store/uiStore'
import { useRunStore } from '../../store/runStore'

/**
 * Last-run summary sourced from `runStore` — live phase/progress while a
 * run streams, then the terminal outcome + issue counts. Links to the
 * Validation view where the full results live.
 */
function RunSummary() {
  const runKind = useRunStore((s) => s.runKind)
  const isRunning = useRunStore((s) => s.isRunning)
  const phase = useRunStore((s) => s.phase)
  const progress = useRunStore((s) => s.progress)
  const terminal = useRunStore((s) => s.terminal)
  const issues = useRunStore((s) => s.issues)

  const kindLabel = runKind === 'validate' ? 'Validate' : 'Generate'

  const errors = issues.filter((i) => i.severity === 'error').length
  const warnings = issues.filter((i) => i.severity === 'warning').length
  const failed = terminal === 'failed' || terminal === 'error' || errors > 0
  const outcome = failed
    ? `${kindLabel} failed`
    : runKind === 'validate'
      ? 'Validated'
      : 'Generated'
  const counts = [
    errors > 0 ? `${errors} error${errors === 1 ? '' : 's'}` : null,
    warnings > 0 ? `${warnings} warning${warnings === 1 ? '' : 's'}` : null,
  ]
    .filter(Boolean)
    .join(' · ')
  const summaryText = `${outcome}${counts ? ` · ${counts}` : ' · no issues'}`

  // Persistent sr-only live region: mounted from app start and updated with
  // text (not attribute) changes, so screen readers announce the run outcome.
  // Only terminal outcomes are announced — per-tick progress would be noise.
  const liveRegion = (
    <span role="status" className="sr-only">
      {runKind && !isRunning && terminal ? summaryText : ''}
    </span>
  )

  if (!runKind) return liveRegion

  if (isRunning) {
    return (
      <>
        {liveRegion}
        <span className="inline-flex min-w-0 items-center gap-1.5 text-muted-foreground">
          <Loader2 className="size-3 shrink-0 animate-spin" aria-hidden="true" />
          <span className="truncate">{phase ?? `${kindLabel} running`}</span>
          {progress && progress.total > 0 && (
            <span className="shrink-0 font-mono tabular-nums">
              {progress.done}/{progress.total}
            </span>
          )}
        </span>
      </>
    )
  }

  const Icon = failed ? CircleX : warnings > 0 ? TriangleAlert : CircleCheck
  const tone = failed ? 'text-error' : warnings > 0 ? 'text-warning' : 'text-success'

  return (
    <>
      {liveRegion}
      <NavLink
        to="/validation"
        title="Open validation results"
        className={cn(
          'inline-flex min-w-0 items-center gap-1.5 hover:text-foreground hover:underline',
          tone,
        )}
      >
        <Icon className="size-3 shrink-0" aria-hidden="true" />
        <span className="truncate">{summaryText}</span>
      </NavLink>
    </>
  )
}

/** h-6 bottom status bar: health · project root · last run · env · version. */
export function StatusBar() {
  const { data: health, isError: healthError, isPending: healthPending } = useHealth()
  const selectedEnv = useUIStore((s) => s.selectedEnv)

  const isHealthy = health?.status === 'healthy' && !healthError
  const healthLabel = healthPending ? 'Connecting…' : isHealthy ? 'Connected' : 'Disconnected'
  const HealthIcon = healthPending ? Loader2 : isHealthy ? Wifi : WifiOff

  return (
    <footer className="flex h-6 shrink-0 items-center gap-4 border-t border-border bg-sidebar px-3 text-2xs text-muted-foreground">
      {/* Health */}
      <span
        role="status"
        className={cn(
          'inline-flex shrink-0 items-center gap-1.5',
          healthPending ? 'text-muted-foreground' : isHealthy ? 'text-success' : 'text-error',
        )}
      >
        <HealthIcon
          className={cn('size-3', healthPending && 'animate-spin')}
          aria-hidden="true"
        />
        {healthLabel}
      </span>

      {/* Project root */}
      {health?.root && (
        <span className="min-w-0 truncate font-mono" title={health.root}>
          {health.root}
        </span>
      )}

      {/* Last run summary */}
      <RunSummary />

      <span className="ml-auto flex shrink-0 items-center gap-2">
        {/* Active environment chip */}
        <Badge variant="outline" className="h-4 rounded-sm px-1.5 text-2xs text-muted-foreground">
          {selectedEnv}
        </Badge>
        {/* LHP version */}
        {health?.version && <span className="font-mono">v{health.version}</span>}
      </span>
    </footer>
  )
}
