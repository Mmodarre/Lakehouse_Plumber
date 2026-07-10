import { create } from 'zustand'
import { useCallback } from 'react'
import { useEventStream } from '../hooks/useEventStream'
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
export type RunTerminal = 'success' | 'failed' | 'error'

export interface RunProgress {
  total: number
  done: number
  current: string | null
}

interface RunState {
  /** Which run is in flight (or was, until reset). */
  runKind: RunKind | null
  /** True while the stream is open. */
  isRunning: boolean
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

  // Actions
  /** Mark a run as started; clears prior run state. */
  begin: (kind: RunKind) => void
  /** Fold one decoded frame into run state. */
  applyFrame: (frame: StreamFrame) => void
  /** Record an out-of-band failure (HTTP/transport error from the hook). */
  fail: (error: Error | ErrorFrame) => void
  /** Mark the stream as finished (clean end). */
  finish: () => void
  /** Reset everything to idle. */
  reset: () => void
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
  isRunning: false,
  phase: null as string | null,
  progress: null as RunProgress | null,
  issues: [] as ValidationIssue[],
  terminal: null as RunTerminal | null,
  errorFrame: null as ErrorFrame | null,
  infoLog: [] as string[],
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

export const useRunStore = create<RunState>((set) => ({
  ...initialState,

  begin: (kind) =>
    set({
      ...initialState,
      runKind: kind,
      isRunning: true,
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

  finish: () =>
    set((s) => ({
      isRunning: false,
      // If the stream ended without an explicit terminal frame, treat it
      // as a clean success (validate/generate streams normally emit a
      // *Completed frame, but guard against a silent close).
      terminal: s.terminal ?? 'success',
    })),

  reset: () => set({ ...initialState }),

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

// ── useRunController — wires useEventStream → runStore ──────────
//
// A thin controller hook. It must be called from a mounted component
// (it owns the transport hook's state/effects); the Header is the
// natural host since it is always mounted in the layout. It reads the
// selected env / pipeline filter from `uiStore` (the same selectors the
// Header renders) so callers only pass overrides when they have them.

export interface RunController {
  isRunning: boolean
  startValidate: (env?: string, pipeline?: string) => void
  startGenerate: (env?: string, pipeline?: string) => void
  abort: () => void
}

export function useRunController(): RunController {
  const stream = useEventStream()
  const begin = useRunStore((s) => s.begin)
  const applyFrame = useRunStore((s) => s.applyFrame)
  const fail = useRunStore((s) => s.fail)
  const finish = useRunStore((s) => s.finish)

  const startRun = useCallback(
    (
      kind: RunKind,
      path: '/api/validate/stream' | '/api/generate/stream',
      env: string,
      pipeline?: string,
    ) => {
      if (stream.isRunning) return
      // The run-config binding (set by the pipeline tab's "Use for runs"
      // toggle, shown in the header chip) applies to BOTH run kinds.
      const { selectedPipelineConfig } = useUIStore.getState()
      begin(kind)
      stream.start(
        { path, env, pipeline, pipeline_config: selectedPipelineConfig ?? undefined },
        {
          onFrame: (frame) => applyFrame(frame),
          onError: (error) => fail(error),
          onDone: () => finish(),
        },
      )
    },
    [stream, begin, applyFrame, fail, finish],
  )

  const startValidate = useCallback(
    (env?: string, pipeline?: string) => {
      const { selectedEnv, pipelineFilter } = useUIStore.getState()
      startRun(
        'validate',
        '/api/validate/stream',
        env ?? selectedEnv,
        pipeline ?? pipelineFilter ?? undefined,
      )
    },
    [startRun],
  )

  const startGenerate = useCallback(
    (env?: string, pipeline?: string) => {
      const { selectedEnv, pipelineFilter } = useUIStore.getState()
      startRun(
        'generate',
        '/api/generate/stream',
        env ?? selectedEnv,
        pipeline ?? pipelineFilter ?? undefined,
      )
    },
    [startRun],
  )

  return {
    isRunning: stream.isRunning,
    startValidate,
    startGenerate,
    abort: stream.abort,
  }
}
