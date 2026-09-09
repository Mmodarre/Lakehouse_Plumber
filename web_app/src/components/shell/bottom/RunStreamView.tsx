import { Square } from 'lucide-react'
import { Button } from '../../ui/button'
import { abortCurrentRun, useRunStore } from '../../../store/runStore'
import { ValidationPanel } from '../../validation/ValidationPanel'

// ── RunStreamView — the bottom-panel Run tab (§6.7 / D12) ───────
//
// Recomposes the existing ValidationPanel (phase indicator, ProgressBar, info
// log lines, terminal state, structured issues) into the bottom dock, adding
// the one UI adjustment §6.7 calls for: a Stop button that cooperatively
// aborts the in-flight run via the shared abort path (abortActiveStream). No
// new run state or backend events — the live feed granularity (phase +
// pipeline + per-flowgroup progress ticks + warnings) is exactly what
// runStore.applyFrame already produces from the existing stream.
//
// The run header's severity-tagged log feed and mm:ss elapsed timer live in
// ValidationPanel (the timer reads runStore.startedAt, set in begin()).

export function RunStreamView() {
  const queued = useRunStore((s) => s.validationQueued)
  const inputs = useRunStore((s) => s.inputs)
  const isRunning = useRunStore((s) => s.isRunning)

  return (
    <div className="flex h-full min-h-0 flex-col">
      {inputs && (
        <p className="border-b border-border px-3 py-1.5 text-xs text-muted-foreground">
          {inputs.env} · {inputs.sandbox ? 'sandbox scope' : inputs.pipeline ?? 'all pipelines'}
          {' · '}{inputs.pipeline_config ?? 'no pipeline config'} · saved files
          {queued && <span className="ml-2 text-info">Validation queued after this run</span>}
        </p>
      )}
      {isRunning && (
        <div className="flex shrink-0 items-center justify-end border-b border-border px-3 py-1.5">
          <Button
            variant="outline"
            size="xs"
            onClick={() => abortCurrentRun()}
            title="Stop the run (finishes the current flowgroup)"
          >
            <Square aria-hidden="true" />
            Stop
          </Button>
        </div>
      )}
      <div className="min-h-0 flex-1 overflow-auto p-3">
        <ValidationPanel />
      </div>
    </div>
  )
}
