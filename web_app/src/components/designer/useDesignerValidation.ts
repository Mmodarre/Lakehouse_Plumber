import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { useRunController, useRunStore } from '@/store/runStore'
import { useUIStore } from '@/store/uiStore'
import { mapIssuesToNodes } from './designerValidation'
import type { DesignerValidation, ValidationNode } from './designerValidation'
import type { ErrorFrame, ValidationIssue } from '@/types/api'

// ── useDesignerValidation — on-demand validate for one flowgroup ─
//
// Reuses the shared run client (`useRunController` → POST /api/validate/stream,
// results in `useRunStore`) rather than opening a second stream. A run is
// scoped to THIS flowgroup's pipeline (pipeline_filter).
//
// `useRunStore` is a SINGLE global bus — the Header, ValidationPanel, and
// useWorkspaceSave (which auto-validates on every docked YAML save) all reset
// and refill `store.issues`. So the designer must NOT render the live store:
// it SNAPSHOTS the store's issue set at the moment its OWN run completes and
// renders that snapshot. A later foreign run that overwrites `store.issues`
// therefore cannot change the designer's verdict or badges. The snapshot is
// still gated by content: `run()` records the validated content and results
// show only while it equals the live content, so a real edit invalidates the
// verdict (on-demand only — no live validation). A re-run replaces the
// snapshot.

export type ValidateStatus = 'idle' | 'running' | 'done'

export interface DesignerValidationState extends DesignerValidation {
  status: ValidateStatus
  /** A run can start (a pipeline is known and nothing is already running). */
  canRun: boolean
  /** The last run ended with a transport/terminal error rather than issues. */
  errored: boolean
  run: () => void
}

export interface UseDesignerValidationArgs {
  pipeline: string
  flowgroup: string
  /** Live flowgroup file content — results invalidate when it changes. */
  content: string | null
  nodes: ValidationNode[]
}

/** The store snapshot captured when our own run completes. */
interface RunResult {
  issues: ValidationIssue[]
  errorFrame: ErrorFrame | null
  /** Content that was validated — results are stale once it changes. */
  content: string | null
}

const NO_RESULTS: DesignerValidation = {
  perNode: new Map(),
  issues: [],
  errorCount: 0,
  warningCount: 0,
}

export function useDesignerValidation({
  pipeline,
  flowgroup,
  content,
  nodes,
}: UseDesignerValidationArgs): DesignerValidationState {
  const { startValidate } = useRunController()
  const env = useUIStore((s) => s.selectedEnv)
  const isRunning = useRunStore((s) => s.isRunning)

  const [phase, setPhase] = useState<'idle' | 'running' | 'done'>('idle')
  const [result, setResult] = useState<RunResult | null>(null)
  const startedRef = useRef(false)
  // Content at run start, read inside the (mount-once) subscription callback.
  const runContentRef = useRef<string | null>(null)

  const run = useCallback(() => {
    if (pipeline === '' || isRunning) return
    startedRef.current = true
    runContentRef.current = content
    setPhase('running')
    startValidate(env, pipeline)
  }, [pipeline, isRunning, content, env, startValidate])

  // Our run's stream closing (store isRunning falling to false) makes the
  // store's issue set authoritative — snapshot it NOW so a later foreign run
  // that overwrites the shared store cannot change our verdict. Reacting via
  // the store subscription (only for runs we started) keeps this out of the
  // render/effect cycle.
  useEffect(() => {
    return useRunStore.subscribe((state, prev) => {
      if (startedRef.current && prev.isRunning && !state.isRunning && state.runKind === 'validate') {
        startedRef.current = false
        setResult({
          issues: state.issues,
          errorFrame: state.errorFrame,
          content: runContentRef.current,
        })
        setPhase('done')
      }
    })
  }, [])

  // Results describe the content that was validated; an edit makes them stale.
  const fresh =
    phase === 'done' && result !== null && result.content !== null && result.content === content
  const status: ValidateStatus = phase === 'running' ? 'running' : fresh ? 'done' : 'idle'

  const mapped = useMemo<DesignerValidation>(
    () => (fresh && result !== null ? mapIssuesToNodes(result.issues, nodes, flowgroup, pipeline) : NO_RESULTS),
    [fresh, result, nodes, flowgroup, pipeline],
  )

  return {
    ...mapped,
    status,
    canRun: pipeline !== '' && !isRunning,
    errored: fresh && result !== null && result.errorFrame !== null,
    run,
  }
}
