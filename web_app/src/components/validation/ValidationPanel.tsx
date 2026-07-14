import { useEffect, useState } from 'react'
import { CircleCheck, CircleX, Loader2, PlayCircle, Timer, TriangleAlert } from 'lucide-react'
import type { LucideIcon } from 'lucide-react'
import { useRunController, useRunStore } from '../../store/runStore'
import type { RunTerminal } from '../../store/runStore'
import { ValidationResults } from './ValidationResults'
import { SandboxRunBadge } from '../sandbox/SandboxRunBadge'
import { EmptyState } from '../common/EmptyState'
import { cn } from '@/lib/utils'

// ── Streaming-log severity tagging ──────────────────────────────
//
// The live `infoLog` is a flat list of server strings. Each line is
// classified by a leading marker (symbol or word) into one of four
// severities, rendered as a tinted tag; the marker is stripped from the
// displayed message. This is purely presentational — no run state changes.

type LogSeverity = 'info' | 'ok' | 'warn' | 'error'

const LOG_SEVERITY_CLASS: Record<LogSeverity, string> = {
  info: 'text-info',
  ok: 'text-success',
  warn: 'text-warning',
  error: 'text-error',
}

// Longer word variants precede shorter ones so the whole marker is consumed
// (e.g. WARNING before WARN, FAILED before FAIL).
const LOG_MARKER =
  /^\s*(✓|✔|✗|✘|⚠️|⚠|▸|»|INFO|SUCCESS|WARNING|WARN|ERROR|FAILURE|FAILED|FAIL|OK)(?![A-Za-z])[:.·\s]*/i

function classifyLogLine(raw: string): { severity: LogSeverity; text: string } {
  const match = raw.match(LOG_MARKER)
  if (!match) return { severity: 'info', text: raw }
  const marker = match[1].toLowerCase()
  const rest = raw.slice(match[0].length)
  const text = rest.length > 0 ? rest : raw.trim()
  let severity: LogSeverity = 'info'
  if (marker === '✓' || marker === '✔' || marker === 'ok' || marker === 'success') {
    severity = 'ok'
  } else if (
    marker === '✗' ||
    marker === '✘' ||
    marker.startsWith('error') ||
    marker.startsWith('fail')
  ) {
    severity = 'error'
  } else if (marker.startsWith('⚠') || marker.startsWith('warn')) {
    severity = 'warn'
  }
  return { severity, text }
}

// Tint a fully-qualified written target (e.g. `main.bronze.customers`) after an
// arrow so the sink table stands out; plainer targets (views) stay uncolored.
function LogMessage({ text }: { text: string }) {
  const arrow = text.match(/(→|->)\s*(\S.*)$/)
  if (!arrow || arrow.index === undefined) return <>{text}</>
  const head = text.slice(0, arrow.index)
  const target = arrow[2].trim()
  const isTable = /^[A-Za-z_]\w*\.[\w.]+$/.test(target)
  return (
    <>
      {head}
      <span className="text-muted-foreground">{arrow[1]} </span>
      {isTable ? <span className="text-kind-write">{target}</span> : target}
    </>
  )
}

function LogLine({ line, active }: { line: string; active: boolean }) {
  const { severity, text } = classifyLogLine(line)
  return (
    <div className="flex items-baseline gap-2">
      <span
        className={cn('w-12 shrink-0 font-semibold uppercase', LOG_SEVERITY_CLASS[severity])}
      >
        {severity}
      </span>
      <span className="min-w-0 flex-1 break-words text-foreground">
        <LogMessage text={text} />
        {active && (
          <Loader2
            className="ml-1 inline-block size-3 animate-spin align-text-bottom text-primary"
            aria-hidden="true"
          />
        )}
      </span>
    </div>
  )
}

// Ticks an mm:ss elapsed counter once a second while mounted. The caller only
// mounts it while a run is in flight, so the interval is torn down on finish.
function ElapsedTimer({ startedAt }: { startedAt: number }) {
  const [now, setNow] = useState(() => Date.now())
  useEffect(() => {
    const id = window.setInterval(() => setNow(Date.now()), 1000)
    return () => window.clearInterval(id)
  }, [])
  const secs = Math.max(0, Math.floor((now - startedAt) / 1000))
  const mm = String(Math.floor(secs / 60)).padStart(2, '0')
  const ss = String(secs % 60).padStart(2, '0')
  return (
    <span
      aria-label="Elapsed time"
      className="ml-auto inline-flex items-center gap-1 font-mono text-xs tabular-nums text-muted-foreground"
    >
      <Timer className="size-3.5 text-faint" aria-hidden="true" />
      {mm}:{ss}
    </span>
  )
}

// ── ValidationPanel — live run view ─────────────────────────
//
// Reads the shared run state from `runStore` (driven by the controller
// mounted in the Header). Shows the current phase, a progress bar, any
// streaming info lines, a terminal status banner, and the structured
// issue list. A run is normally *triggered* from the Header's
// Validate/Generate buttons; the idle empty state offers an in-place
// "Validate project" action through the same run-controller flow.

function terminalBanner(
  terminal: RunTerminal,
  kind: string,
  errorTitle: string | null,
  warningCount: number,
): { text: string; className: string; icon: LucideIcon; iconClassName: string } {
  if (terminal === 'success') {
    if (warningCount > 0) {
      return {
        text: `${kind} completed with ${warningCount} ${warningCount === 1 ? 'warning' : 'warnings'}`,
        className: 'border-warning/25 bg-warning/12',
        icon: TriangleAlert,
        iconClassName: 'text-warning',
      }
    }
    return {
      text: `${kind} completed successfully`,
      className: 'border-success/25 bg-success/12',
      icon: CircleCheck,
      iconClassName: 'text-success',
    }
  }
  if (terminal === 'error') {
    return {
      text: errorTitle ?? `${kind} errored`,
      className: 'border-error/25 bg-error/12',
      icon: CircleX,
      iconClassName: 'text-error',
    }
  }
  return {
    text: `${kind} failed — see issues below`,
    className: 'border-error/25 bg-error/12',
    icon: CircleX,
    iconClassName: 'text-error',
  }
}

function ProgressBar({
  total,
  done,
  current,
}: {
  total: number
  done: number
  current: string | null
}) {
  const pct = total > 0 ? Math.round((done / total) * 100) : 0
  return (
    <div className="space-y-1">
      <div className="flex items-center justify-between text-xs text-muted-foreground">
        <span>{current ?? 'Working…'}</span>
        <span className="font-mono tabular-nums">
          {done}/{total}
        </span>
      </div>
      <div
        role="progressbar"
        aria-label="Run progress"
        aria-valuenow={done}
        aria-valuemin={0}
        aria-valuemax={total}
        className="h-1.5 w-full overflow-hidden rounded-full bg-secondary"
      >
        <div
          className="h-full rounded-full bg-primary transition-[width] duration-300 ease-out"
          style={{ width: `${pct}%` }}
        />
      </div>
    </div>
  )
}

export function ValidationPanel() {
  const runKind = useRunStore((s) => s.runKind)
  const isRunning = useRunStore((s) => s.isRunning)
  const phase = useRunStore((s) => s.phase)
  const progress = useRunStore((s) => s.progress)
  const issues = useRunStore((s) => s.issues)
  const terminal = useRunStore((s) => s.terminal)
  const errorFrame = useRunStore((s) => s.errorFrame)
  const infoLog = useRunStore((s) => s.infoLog)
  const startedAt = useRunStore((s) => s.startedAt)
  const { startValidate } = useRunController()

  const kindLabel = runKind === 'generate' ? 'Generation' : 'Validation'
  const warningCount = issues.filter((i) => i.severity === 'warning').length

  // Idle: nothing has been run yet this session.
  if (runKind === null) {
    return (
      <div className="rounded-lg border border-border bg-card">
        <EmptyState
          icon={PlayCircle}
          title="No runs yet"
          message="Validate the project to see progress and structured issues here."
          action={{ label: 'Validate project', onClick: () => startValidate() }}
        />
      </div>
    )
  }

  const banner = terminalBanner(
    terminal ?? 'success',
    kindLabel,
    errorFrame?.title ?? null,
    warningCount,
  )
  const BannerIcon = banner.icon

  return (
    <div className="space-y-4">
      {/* Sandbox-mode marker (renders only for a sandbox run) */}
      <SandboxRunBadge />

      {/* Live phase + progress */}
      {isRunning && (
        <div className="space-y-3 rounded-lg border border-border bg-card px-4 py-3">
          <div className="flex items-center gap-2 text-sm font-medium text-foreground">
            <Loader2 className="size-3.5 animate-spin text-primary" aria-hidden="true" />
            <span>{phase ? `${kindLabel}: ${phase}` : `${kindLabel} running…`}</span>
            {/* Elapsed timer only in a live run (startedAt set by begin()); safe
                in the standalone validation view, which shares this store. */}
            {startedAt !== null && <ElapsedTimer startedAt={startedAt} />}
          </div>
          {progress && (
            <ProgressBar
              total={progress.total}
              done={progress.done}
              current={progress.current}
            />
          )}
        </div>
      )}

      {/* Streaming info lines — a monospace feed with per-line severity tags
          and a spinner on the active (last) line while the run is in flight. */}
      {infoLog.length > 0 && (
        <div
          role="log"
          aria-label="Run log stream"
          className="max-h-64 space-y-0.5 overflow-auto rounded-lg bg-muted/40 p-3 font-mono text-xs"
        >
          {infoLog.map((line, i) => (
            <LogLine key={i} line={line} active={isRunning && i === infoLog.length - 1} />
          ))}
        </div>
      )}

      {/* Terminal status banner — a live region so the run outcome is
          announced (alert for failures, polite status for success) */}
      {!isRunning && terminal && (
        <div
          role={terminal === 'success' ? 'status' : 'alert'}
          aria-live={terminal === 'success' ? 'polite' : 'assertive'}
          className={cn(
            'flex items-center gap-2 rounded-lg border px-4 py-3 text-sm font-medium text-foreground',
            banner.className,
          )}
        >
          <BannerIcon
            className={cn('size-4 shrink-0', banner.iconClassName)}
            aria-hidden="true"
          />
          {banner.text}
        </div>
      )}

      {/* Terminal error detail (transport / non-issue failure) */}
      {!isRunning && errorFrame && (
        <div className="rounded-lg border border-error/25 bg-error/12 px-3 py-2 text-xs text-foreground">
          <span className="font-mono text-2xs text-error">{errorFrame.code}</span>
          {errorFrame.details && (
            <div className="mt-0.5 whitespace-pre-wrap text-muted-foreground">
              {errorFrame.details}
            </div>
          )}
          {errorFrame.suggestions.length > 0 && (
            <ul className="mt-1 list-inside list-disc space-y-0.5 text-muted-foreground">
              {errorFrame.suggestions.map((s, i) => (
                <li key={i}>{s}</li>
              ))}
            </ul>
          )}
        </div>
      )}

      {/* Structured issues */}
      <ValidationResults issues={issues} />
    </div>
  )
}
