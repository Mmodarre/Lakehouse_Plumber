import { useEffect, useRef } from 'react'
import { useRunStore } from '../store/runStore'
import { fetchRuns, fetchRun } from '../api/runs'
import type { ValidationIssue } from '../types/api'

// ── useHydrateProblems — seed Problems from the last validation (§6.7) ──
//
// runStore.issues is empty on a fresh boot (a run only fills it live). This
// one-shot hook (mount once, in BottomPanel) restores the last validation's
// issues so the Problems panel is useful before the user re-runs: GET /api/runs
// → newest non-running `validate` → GET that run WITH events → pull the full
// ValidationIssueView dicts out of the persisted terminal ValidationCompleted
// frame → runStore.hydrateIssues. There is NO boot-validate and NO new
// endpoint. A live run later replaces the hydrated state (runStore.begin
// clears the marker).

interface EventFrame {
  type?: unknown
  response?: { pipeline_responses?: Record<string, { issues?: ValidationIssue[] }> }
}

/** Extract every issue from the last persisted `ValidationCompleted` frame. The
 * flat RunIssue rows lack pipeline/flowgroup names; the embedded frame's
 * `ValidationIssueView` dicts do not (proven in run_recorder). */
export function extractValidationIssues(
  events: ReadonlyArray<Record<string, unknown>>,
): ValidationIssue[] {
  const frame = [...events]
    .reverse()
    .find((e) => (e as EventFrame).type === 'ValidationCompleted') as EventFrame | undefined
  const responses = frame?.response?.pipeline_responses
  if (!responses) return []
  const out: ValidationIssue[] = []
  for (const pipeline of Object.values(responses)) {
    for (const issue of pipeline?.issues ?? []) out.push(issue)
  }
  return out
}

export function useHydrateProblems(): void {
  const done = useRef(false)
  useEffect(() => {
    if (done.current) return
    done.current = true
    let cancelled = false

    void (async () => {
      try {
        const { runs } = await fetchRuns(50)
        const latest = runs.find((r) => r.kind === 'validate' && r.status !== 'running')
        if (!latest || cancelled) return
        const detail = await fetchRun(latest.run_id, true)
        if (cancelled) return
        const issues = extractValidationIssues(detail.events ?? [])
        // Don't overwrite anything already surfaced this session (a live run,
        // an earlier hydration, or a synthetic editor issue).
        const s = useRunStore.getState()
        if (s.isRunning || s.hydratedFrom || s.issues.length > 0) return
        s.hydrateIssues(issues, {
          runId: latest.run_id,
          startedAt: latest.started_at,
          env: latest.env,
          pipeline: latest.pipeline,
        })
      } catch {
        // Best-effort restoration — a missing/failed history read just leaves
        // the Problems panel empty until the next run.
      }
    })()

    return () => {
      cancelled = true
    }
  }, [])
}
