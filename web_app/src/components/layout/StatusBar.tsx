import { CircleCheck, CircleX, FolderGit2, Loader2, TriangleAlert } from 'lucide-react'
import { cn } from '../../lib/utils'
import { useHealth, useProject } from '../../hooks/useProject'
import { useUIStore } from '../../store/uiStore'
import { useRunStore } from '../../store/runStore'

// ── StatusBar — 26px accent footer for the unified workspace (§3) ──
//
// Accent background + accent-ink text (tokens --primary / --primary-foreground
// ≡ the schematic accent / accent-ink). Content: project · run counts ·
// validation state · running progress · env · right `LHP <version>`. Same data
// sources as before (runStore / useHealth / useProject / uiStore); relocated
// into AppShell. Everything reads in accent-ink — state is carried by icon +
// text, not tone colour (unreadable on the accent fill).

/** One status-bar segment; segments after the first get a hairline divider. */
function Segment({ className, ...props }: React.HTMLAttributes<HTMLSpanElement>) {
  return (
    <span
      className={cn(
        'inline-flex min-w-0 items-center gap-1.5 px-2.5 whitespace-nowrap',
        'border-l border-primary-foreground/20 first:border-l-0',
        className,
      )}
      {...props}
    />
  )
}

/**
 * Last-run summary sourced from `runStore` — live phase/progress while a run
 * streams, then the terminal outcome + issue counts.
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
      <Segment>
        {liveRegion}
        <Loader2 className="size-3 shrink-0 animate-spin" aria-hidden="true" />
        <span className="truncate">{phase ?? `${kindLabel} running`}</span>
        {progress && progress.total > 0 && (
          <span className="shrink-0 font-mono tabular-nums">
            {progress.done}/{progress.total}
          </span>
        )}
      </Segment>
    )
  }

  const Icon = failed ? CircleX : warnings > 0 ? TriangleAlert : CircleCheck

  return (
    <Segment title={summaryText}>
      {liveRegion}
      <Icon className="size-3 shrink-0" aria-hidden="true" />
      <span className="truncate">{summaryText}</span>
    </Segment>
  )
}

/** 26px accent status bar: project · run summary · (right) env · version. */
export function StatusBar() {
  const { data: health } = useHealth()
  const { data: project } = useProject()
  const selectedEnv = useUIStore((s) => s.selectedEnv)

  const projectLabel = project?.name ?? health?.root

  return (
    <footer className="flex h-[26px] shrink-0 items-center bg-primary text-2xs font-medium text-primary-foreground">
      {projectLabel && (
        <Segment title={health?.root ?? projectLabel}>
          <FolderGit2 className="size-3 shrink-0" aria-hidden="true" />
          <span className="truncate">{projectLabel}</span>
        </Segment>
      )}

      {/* Run counts · validation state · running progress */}
      <RunSummary />

      <span className="ml-auto flex shrink-0 items-center">
        <Segment>
          <span className="font-mono">env: {selectedEnv}</span>
        </Segment>
        {health?.version && (
          <Segment>
            <span className="font-mono">LHP v{health.version}</span>
          </Segment>
        )}
      </span>
    </footer>
  )
}
