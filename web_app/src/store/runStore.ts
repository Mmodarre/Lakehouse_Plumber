import { create } from 'zustand'
import { useCallback } from 'react'
import { useQueryClient, type QueryClient } from '@tanstack/react-query'
import { abortActiveStream, startEventStream, type StartOptions } from '../hooks/useEventStream'
import { useLayoutStore } from './layoutStore'
import { useUIStore } from './uiStore'
import { ApiError } from '../api/client'
import type {
  ErrorFrame,
  StreamFrame,
  ValidationIssue,
} from '../types/api'

// ── runStore — single source of truth for a validate/generate run ──
//
// The transport hook (`useEventStream`) only decodes frames. This store
// owns the *meaning* of a run: which kind is in flight, the current
// phase, progress, the accumulating list of structured issues, and the
// terminal outcome. Every frame→state transition lives in `applyFrame`
// so there is exactly one reducer that components, panels, and the
// problems list all read from.
//
// Issues are collected two ways:
//   • Live — `WarningEmitted` and `PipelineFailed` frames are synthesized
//     into issue-like entries so failures/warnings surface *during* the
//     run rather than only at the end.
//   • Authoritative — a terminal `ValidationCompleted` / `GenerationCompleted`
//     frame carries the full per-pipeline issue set; when it arrives we
//     replace the live-synthesized list with that authoritative set
//     (across all pipelines) to avoid double-counting.

export type RunKind = 'validate' | 'generate'
/** What started a run: a user action, or the editor acting on its own. */
export type RunTrigger = 'manual' | 'auto'
export type RunTerminal = 'success' | 'failed' | 'error' | 'stopped' | 'incomplete'

export interface RunProgress {
  total: number
  done: number
  current: string | null
}

/** Provenance of Problems hydrated from run history (§6.7). Set by
 * `hydrateIssues`; cleared whenever a live run starts (`begin` resets it). */
export interface HydratedRunMeta {
  runId: string
  startedAt: string | null
  env: string | null
  pipeline: string | null
}

interface RunState {
  /** Which run is in flight (or was, until reset). */
  runKind: RunKind | null
  /** True when the current/last run was launched in developer-sandbox mode
   * (scope + namespace from .lhp/profile.yaml). Drives the Sandbox run badge. */
  sandbox: boolean
  /** True while the stream is open. */
  isRunning: boolean
  /** Wall-clock ms (`Date.now()`) when the current run began; null when idle.
   * Drives the run header's elapsed timer, which only ticks while `isRunning`. */
  startedAt: number | null
  /** Most recent phase label from the server (e.g. "Validating"). */
  phase: string | null
  /** Latest progress snapshot, if the run reports progress. */
  progress: RunProgress | null
  /** Structured diagnostics — live-synthesized then replaced by the
   * authoritative terminal set. */
  issues: ValidationIssue[]
  /** Terminal outcome once the stream finishes; null while running. */
  terminal: RunTerminal | null
  /** Set when the run failed with a transport/terminal error frame. */
  errorFrame: ErrorFrame | null
  /** Free-form info/status lines surfaced during the run. */
  infoLog: string[]
  /** Non-null when `issues` were hydrated from a past validation run rather
   * than produced live this session; drives the Problems "from last
   * validation" note. Cleared by `begin`/`reset`. */
  hydratedFrom: HydratedRunMeta | null

  /** Monotonic session identity prevents a prior stream/hydration overwriting a newer run. */
  runId: number
  inputs: StartOptions | null
  validationQueued: boolean

  // Actions
  /** Mark a run as started; clears prior run state. `sandbox` records whether
   * the run was launched in developer-sandbox mode. */
  begin: (kind: RunKind, sandbox?: boolean) => void
  /** Fold one decoded frame into run state. */
  applyFrame: (frame: StreamFrame) => void
  /** Record an out-of-band failure (HTTP/transport error from the hook). */
  fail: (error: Error | ErrorFrame) => void
  /** Finish with an explicit result, stopped state, or incomplete outcome. */
  finish: (info?: { aborted: boolean }) => void
  /** Reset everything to idle. */
  reset: () => void
  /**
   * Populate `issues` from a past validation run's persisted issues WITHOUT
   * touching phase/running/terminal state (do NOT replay via `applyFrame` — it
   * mutates those). Records `meta` as the hydration marker. No-ops while a live
   * run is in flight so a running stream always wins.
   */
  hydrateIssues: (issues: ValidationIssue[], meta: HydratedRunMeta) => void
  /**
   * Set or clear a synthetic YAML-syntax issue for a file. When `issue` is
   * provided, an `error`-severity entry (code `YAML-SYNTAX`) for `filePath`
   * replaces any existing one and surfaces in the Problems panel; passing
   * `null` clears it. Used by the editor modals when a save persists but the
   * YAML failed to parse (and on a subsequent clean save to clear it).
   */
  setSyntheticSyntaxIssue: (
    filePath: string,
    issue: { line: number; column: number; message: string } | null,
  ) => void
}

/** Code used to tag synthetic YAML-syntax-error issues so they can be
 * found and replaced/cleared independently of run-produced issues. */
const YAML_SYNTAX_ISSUE_CODE = 'YAML-SYNTAX'

const initialState = {
  runKind: null as RunKind | null,
  sandbox: false,
  isRunning: false,
  startedAt: null as number | null,
  phase: null as string | null,
  progress: null as RunProgress | null,
  issues: [] as ValidationIssue[],
  terminal: null as RunTerminal | null,
  errorFrame: null as ErrorFrame | null,
  infoLog: [] as string[],
  hydratedFrom: null as HydratedRunMeta | null,
  runId: 0,
  inputs: null as StartOptions | null,
  validationQueued: false,
}

/** A non-error `ErrorFrame` shape coerced from an `ApiError`/`Error`. */
function errorFrameFromError(error: Error | ErrorFrame): ErrorFrame {
  if (!(error instanceof Error)) return error
  if (error instanceof ApiError) {
    return {
      type: 'error',
      code: error.code,
      title: error.message,
      details: null,
      suggestions: error.suggestions,
      context: {},
      doc_link: null,
    }
  }
  return {
    type: 'error',
    code: 'STREAM_ERROR',
    title: error.message,
    details: null,
    suggestions: [],
    context: {},
    doc_link: null,
  }
}

/** Collect the authoritative issue set across all pipelines in a
 * terminal `ValidationCompleted` response. */
function collectValidationIssues(
  responses: Record<string, { issues: ValidationIssue[] }>,
): ValidationIssue[] {
  const all: ValidationIssue[] = []
  for (const pipeline of Object.values(responses)) {
    all.push(...pipeline.issues)
  }
  return all
}

/** Collect issues across all pipelines in a terminal `GenerationCompleted`
 * response. Generation responses carry a single optional `error` issue
 * per pipeline rather than a list. */
function collectGenerationIssues(
  responses: Record<string, { error: ValidationIssue | null }>,
): ValidationIssue[] {
  const all: ValidationIssue[] = []
  for (const pipeline of Object.values(responses)) {
    if (pipeline.error) all.push(pipeline.error)
  }
  return all
}

let nextRunId = 0

export const useRunStore = create<RunState>((set) => ({
  ...initialState,

  begin: (kind, sandbox = false) =>
    set({
      ...initialState,
      runId: ++nextRunId,
      runKind: kind,
      sandbox,
      isRunning: true,
      startedAt: Date.now(),
    }),

  applyFrame: (frame) =>
    set((s) => {
      switch (frame.type) {
        case 'OperationStarted':
          return { phase: null }

        case 'PhaseStarted':
          return { phase: frame.phase }

        case 'PhaseCompleted':
          // Keep the phase label; PhaseStarted of the next phase replaces it.
          return {}

        case 'PipelineStarted':
          return { progress: { ...(s.progress ?? { total: 0, done: 0 }), current: frame.pipeline } }

        case 'PipelineCompleted':
          return {}

        case 'PipelineFailed': {
          // Synthesize an error issue so the failure surfaces live.
          const synthesized: ValidationIssue = {
            code: frame.code,
            category: 'pipeline',
            severity: 'error',
            title: `Pipeline failed: ${frame.pipeline}`,
            details: frame.message,
            pipeline_name: frame.pipeline,
            flowgroup_name: null,
            file_path: null,
            suggestions: [],
            context: {},
            doc_link: null,
          }
          return { issues: [...s.issues, synthesized] }
        }

        case 'WarningEmitted': {
          // Synthesize a warning issue so warnings surface live.
          const synthesized: ValidationIssue = {
            code: frame.code,
            category: frame.category,
            severity: 'warning',
            title: frame.message,
            details: null,
            pipeline_name: null,
            flowgroup_name: frame.flowgroup,
            file_path: frame.file,
            suggestions: [],
            context: {},
            doc_link: null,
          }
          return { issues: [...s.issues, synthesized] }
        }

        case 'ValidationCompleted': {
          const issues = collectValidationIssues(frame.response.pipeline_responses)
          return {
            issues,
            terminal: frame.response.success ? 'success' : 'failed',
          }
        }

        case 'GenerationCompleted': {
          const issues = collectGenerationIssues(frame.response.pipeline_responses)
          return {
            issues,
            terminal: frame.response.success ? 'success' : 'failed',
          }
        }

        case 'progress':
          return {
            progress: {
              total: frame.total,
              done: frame.done,
              current: frame.current,
            },
          }

        case 'info':
          return { infoLog: [...s.infoLog, frame.message] }

        case 'error':
          return { errorFrame: frame, terminal: 'error' }

        default:
          return {}
      }
    }),

  fail: (error) =>
    set((s) => ({
      errorFrame: errorFrameFromError(error),
      // A terminal `error` frame may have already set `terminal`; don't
      // clobber it, but ensure a failure outcome is recorded.
      terminal: s.terminal ?? 'error',
    })),

  finish: (info) =>
    set((s) => ({
      isRunning: false,
      terminal: s.terminal ?? (info?.aborted ? 'stopped' : 'incomplete'),
    })),

  reset: () => set({ ...initialState }),

  hydrateIssues: (issues, meta) =>
    set((s) =>
      // A live/finished run this session owns the Problems list; never clobber
      // it with history. Only phase/running-untouched fields change here.
      s.runKind !== null || s.isRunning ? {} : { issues, hydratedFrom: meta },
    ),

  setSyntheticSyntaxIssue: (filePath, issue) =>
    set((s) => {
      // Drop any prior synthetic syntax issue for this file.
      const kept = s.issues.filter(
        (i) => !(i.code === YAML_SYNTAX_ISSUE_CODE && i.file_path === filePath),
      )
      if (issue === null) {
        // Nothing changed → keep the same array reference to avoid a needless
        // re-render of Problems consumers.
        return kept.length === s.issues.length ? {} : { issues: kept }
      }
      const synthesized: ValidationIssue = {
        code: YAML_SYNTAX_ISSUE_CODE,
        category: 'syntax',
        severity: 'error',
        title: issue.message,
        details: `${filePath}:${issue.line}:${issue.column}`,
        pipeline_name: null,
        flowgroup_name: null,
        file_path: filePath,
        suggestions: [],
        context: { line: issue.line, column: issue.column },
        doc_link: null,
      }
      return { issues: [...kept, synthesized] }
    }),
}))

// One persistent coordinator. Calling views only request operations; unmounting
// those views never cancels the operation. Auto-validation waits behind active work.
export interface RunController {
  isRunning: boolean
  startValidate: (env?: string, pipeline?: string, trigger?: RunTrigger) => void
  startGenerate: (env?: string, pipeline?: string) => void
  queueValidate: (env?: string, pipeline?: string, trigger?: RunTrigger) => void
  abort: () => void
}

let queuedValidation: { options: StartOptions; queryClient: QueryClient } | null = null

export function abortCurrentRun(): void {
  queuedValidation = null
  useRunStore.setState({ validationQueued: false })
  abortActiveStream()
}

export function captureRunInputs(
  kind: RunKind,
  env?: string,
  pipeline?: string,
  trigger: RunTrigger = 'manual',
): StartOptions {
  const ui = useUIStore.getState()
  const sandbox = ui.sandboxEnabled && pipeline === undefined
  return {
    path: kind === 'validate' ? '/api/validate/stream' : '/api/generate/stream',
    env: env ?? ui.selectedEnv,
    pipeline: sandbox ? undefined : pipeline ?? ui.pipelineFilter ?? undefined,
    pipeline_config: ui.selectedPipelineConfig ?? undefined,
    ...(sandbox ? { sandbox: true } : {}),
    // Spread only for the non-default value: a user-initiated run carries no
    // `trigger` key and the backend applies its `manual` default, so the
    // manual wire body stays free of telemetry fields.
    ...(trigger === 'auto' ? { trigger: 'auto' as const } : {}),
  }
}

export function startRunWithInputs(options: StartOptions, queryClient: QueryClient): void {
  if (useRunStore.getState().isRunning || !options.env) return
  if (options.path === '/api/generate/stream' && useLayoutStore.getState().viewerMode) return
  const kind = options.path === '/api/validate/stream' ? 'validate' : 'generate'
  useRunStore.getState().begin(kind, options.sandbox)
  useRunStore.setState({ inputs: options })
  const runId = useRunStore.getState().runId
  const isCurrent = () => useRunStore.getState().runId === runId
  const controller = startEventStream(options, {
    onFrame: (frame) => { if (isCurrent()) useRunStore.getState().applyFrame(frame) },
    onError: (error) => { if (isCurrent()) useRunStore.getState().fail(error) },
    onDone: (info) => {
      if (!isCurrent()) return
      useRunStore.getState().finish(info)
      const next = queuedValidation
      queuedValidation = null
      useRunStore.setState({ validationQueued: false })
      if (next && !info.aborted) startRunWithInputs(next.options, next.queryClient)
    },
  }, queryClient)
  if (!controller) {
    useRunStore.getState().fail(new Error('Another operation is still finishing. Try again.'))
    useRunStore.getState().finish()
  }
}

export function useRunController(): RunController {
  const queryClient = useQueryClient()
  const isRunning = useRunStore((s) => s.isRunning)
  const startValidate = useCallback((env?: string, pipeline?: string, trigger?: RunTrigger) => {
    startRunWithInputs(captureRunInputs('validate', env, pipeline, trigger), queryClient)
  }, [queryClient])
  const startGenerate = useCallback((env?: string, pipeline?: string) => {
    startRunWithInputs(captureRunInputs('generate', env, pipeline), queryClient)
  }, [queryClient])
  const queueValidate = useCallback((env?: string, pipeline?: string, trigger?: RunTrigger) => {
    const options = captureRunInputs('validate', env, pipeline, trigger)
    if (!useRunStore.getState().isRunning) {
      startRunWithInputs(options, queryClient)
      return
    }
    // Multiple saved pipelines require project-wide validation. Keep the
    // latest environment/config snapshot, and never discard an earlier scope.
    if (queuedValidation && queuedValidation.options.pipeline !== options.pipeline) {
      options.pipeline = undefined
    }
    queuedValidation = { options, queryClient }
    useRunStore.setState({ validationQueued: true })
  }, [queryClient])
  return { isRunning, startValidate, startGenerate, queueValidate, abort: abortCurrentRun }
}
