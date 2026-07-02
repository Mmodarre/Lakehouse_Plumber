import { useRunStore } from '../../store/runStore'
import type { RunTerminal } from '../../store/runStore'
import { ValidationResults } from './ValidationResults'

// ── ValidationPanel — live run view ─────────────────────────
//
// Reads the shared run state from `runStore` (driven by the controller
// mounted in the Header). Shows the current phase, a progress bar, any
// streaming info lines, a terminal status banner, and the structured
// issue list. The run itself is *triggered* from the Header's
// Validate/Generate buttons — this panel is the read-side view.

function terminalBanner(
  terminal: RunTerminal,
  kind: string,
  errorTitle: string | null,
): { text: string; className: string } {
  if (terminal === 'success') {
    return {
      text: `${kind} completed successfully`,
      className: 'bg-green-50 text-green-800 border-green-200',
    }
  }
  if (terminal === 'error') {
    return {
      text: errorTitle ?? `${kind} errored`,
      className: 'bg-red-50 text-red-800 border-red-200',
    }
  }
  return {
    text: `${kind} failed — see issues below`,
    className: 'bg-red-50 text-red-800 border-red-200',
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
      <div className="flex items-center justify-between text-xs text-slate-600">
        <span>{current ?? 'Working…'}</span>
        <span className="tabular-nums">
          {done}/{total}
        </span>
      </div>
      <div className="h-1.5 w-full overflow-hidden rounded-full bg-slate-200">
        <div
          className="h-full rounded-full bg-blue-600 transition-all"
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

  const kindLabel = runKind === 'generate' ? 'Generation' : 'Validation'

  // Idle: nothing has been run yet this session.
  if (runKind === null) {
    return (
      <div className="rounded border border-slate-200 bg-white px-4 py-6 text-center text-sm text-slate-500">
        Use the <span className="font-medium text-slate-700">Validate</span> or{' '}
        <span className="font-medium text-slate-700">Generate</span> button in the
        header to start a run.
      </div>
    )
  }

  return (
    <div className="space-y-4">
      {/* Live phase + progress */}
      {isRunning && (
        <div className="space-y-3 rounded border border-slate-200 bg-white px-4 py-3">
          <div className="flex items-center gap-2 text-sm font-medium text-slate-700">
            <span className="h-3.5 w-3.5 animate-spin rounded-full border-2 border-slate-300 border-t-blue-600" />
            {phase ? `${kindLabel}: ${phase}` : `${kindLabel} running…`}
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

      {/* Streaming info lines */}
      {infoLog.length > 0 && (
        <div className="space-y-0.5 rounded border border-slate-200 bg-slate-50 px-3 py-2 font-mono text-[11px] text-slate-600">
          {infoLog.map((line, i) => (
            <div key={i}>{line}</div>
          ))}
        </div>
      )}

      {/* Terminal status banner */}
      {!isRunning && terminal && (
        <div
          className={`rounded-lg border px-4 py-3 text-sm font-medium ${
            terminalBanner(terminal, kindLabel, errorFrame?.title ?? null).className
          }`}
        >
          {terminalBanner(terminal, kindLabel, errorFrame?.title ?? null).text}
        </div>
      )}

      {/* Terminal error detail (transport / non-issue failure) */}
      {!isRunning && errorFrame && (
        <div className="rounded border border-red-200 bg-red-50 px-3 py-2 text-xs text-red-700">
          <span className="font-mono text-[10px]">{errorFrame.code}</span>
          {errorFrame.details && (
            <div className="mt-0.5 whitespace-pre-wrap">{errorFrame.details}</div>
          )}
          {errorFrame.suggestions.length > 0 && (
            <ul className="mt-1 list-inside list-disc space-y-0.5">
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
