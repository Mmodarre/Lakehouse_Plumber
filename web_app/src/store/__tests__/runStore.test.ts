import { beforeEach, describe, expect, it } from 'vitest'
import { useRunStore } from '@/store/runStore'
import { ApiError } from '@/api/client'
import type {
  ErrorFrame,
  GenerationResult,
  StreamFrame,
  ValidationIssue,
  ValidationResult,
} from '@/types/api'

// ── Frame / DTO builders ─────────────────────────────────────────

function issue(over: Partial<ValidationIssue> = {}): ValidationIssue {
  return {
    code: 'LHP-VAL-001',
    category: 'config',
    severity: 'error',
    title: 'Something is wrong',
    details: null,
    pipeline_name: null,
    flowgroup_name: null,
    file_path: null,
    suggestions: [],
    context: {},
    doc_link: null,
    ...over,
  }
}

function validationResult(issues: ValidationIssue[], success: boolean): ValidationResult {
  return { success, issues, validated_pipelines: [], error_message: null }
}

function validationCompleted(
  pipelines: Record<string, ValidationResult>,
  success: boolean,
): StreamFrame {
  return {
    type: 'ValidationCompleted',
    response: {
      success,
      pipeline_responses: pipelines,
      total_errors: 0,
      total_warnings: 0,
      validated_pipelines: Object.keys(pipelines),
      error_message: null,
      error_code: null,
    },
  }
}

function generationResult(error: ValidationIssue | null): GenerationResult {
  return {
    success: error === null,
    generated_filenames: [],
    files_written: 0,
    total_flowgroups: 0,
    output_location: null,
    performance_info: {},
    duration_s: 0.1,
    error_message: null,
    error_code: null,
    error,
  }
}

function generationCompleted(
  pipelines: Record<string, GenerationResult>,
  success: boolean,
): StreamFrame {
  return {
    type: 'GenerationCompleted',
    response: {
      success,
      pipeline_responses: pipelines,
      total_files_written: 0,
      aggregate_generated_filenames: [],
      output_location: null,
      error_message: null,
      error_code: null,
    },
  }
}

const store = () => useRunStore.getState()

beforeEach(() => {
  store().reset()
})

// ── Tests ────────────────────────────────────────────────────────

describe('runStore.begin', () => {
  it('clears prior run state and marks the run in flight', () => {
    store().applyFrame({ type: 'info', code: 'X', message: 'stale' })
    store().begin('validate')

    const s = store()
    expect(s.runKind).toBe('validate')
    expect(s.isRunning).toBe(true)
    expect(s.phase).toBeNull()
    expect(s.issues).toEqual([])
    expect(s.infoLog).toEqual([])
    expect(s.terminal).toBeNull()
  })
})

describe('runStore.applyFrame', () => {
  it('tracks the current phase from PhaseStarted and keeps it on PhaseCompleted', () => {
    store().begin('validate')
    store().applyFrame({ type: 'PhaseStarted', phase: 'Validating' })
    expect(store().phase).toBe('Validating')

    store().applyFrame({ type: 'PhaseCompleted', phase: 'Validating', duration_s: 0.2, success: true })
    expect(store().phase).toBe('Validating')
  })

  it('records progress frames', () => {
    store().begin('generate')
    store().applyFrame({ type: 'progress', total: 4, done: 1, current: 'bronze' })
    expect(store().progress).toEqual({ total: 4, done: 1, current: 'bronze' })
  })

  it('PipelineStarted sets current without losing progress counters', () => {
    store().begin('generate')
    store().applyFrame({ type: 'progress', total: 4, done: 2, current: 'bronze' })
    store().applyFrame({ type: 'PipelineStarted', pipeline: 'silver' })
    expect(store().progress).toEqual({ total: 4, done: 2, current: 'silver' })
  })

  it('synthesizes a live warning issue from WarningEmitted', () => {
    store().begin('validate')
    store().applyFrame({
      type: 'WarningEmitted',
      message: 'Deprecated field',
      code: 'LHP-DEP-002',
      category: 'deprecation',
      file: 'pipelines/orders.yaml',
      flowgroup: 'orders',
    })

    expect(store().issues).toHaveLength(1)
    expect(store().issues[0]).toMatchObject({
      severity: 'warning',
      code: 'LHP-DEP-002',
      title: 'Deprecated field',
      file_path: 'pipelines/orders.yaml',
      flowgroup_name: 'orders',
    })
  })

  it('synthesizes a live error issue from PipelineFailed', () => {
    store().begin('generate')
    store().applyFrame({
      type: 'PipelineFailed',
      pipeline: 'bronze',
      code: 'LHP-GEN-001',
      message: 'boom',
    })

    expect(store().issues).toHaveLength(1)
    expect(store().issues[0]).toMatchObject({
      severity: 'error',
      code: 'LHP-GEN-001',
      title: 'Pipeline failed: bronze',
      details: 'boom',
      pipeline_name: 'bronze',
    })
  })

  it('ValidationCompleted replaces live issues with the authoritative set', () => {
    store().begin('validate')
    store().applyFrame({
      type: 'WarningEmitted',
      message: 'live warning',
      code: 'W',
      category: 'c',
      file: null,
      flowgroup: null,
    })

    const authoritative = [issue({ code: 'E1' }), issue({ code: 'E2', severity: 'warning' })]
    store().applyFrame(
      validationCompleted(
        {
          bronze: validationResult([authoritative[0]], false),
          silver: validationResult([authoritative[1]], true),
        },
        false,
      ),
    )

    const s = store()
    expect(s.issues.map((i) => i.code)).toEqual(['E1', 'E2']) // live 'W' replaced
    expect(s.terminal).toBe('failed')
  })

  it('ValidationCompleted with success=true terminates as success', () => {
    store().begin('validate')
    store().applyFrame(validationCompleted({ bronze: validationResult([], true) }, true))
    expect(store().terminal).toBe('success')
    expect(store().issues).toEqual([])
  })

  it('GenerationCompleted collects only the non-null per-pipeline errors', () => {
    store().begin('generate')
    const err = issue({ code: 'LHP-GEN-042', pipeline_name: 'silver' })
    store().applyFrame(
      generationCompleted(
        { bronze: generationResult(null), silver: generationResult(err) },
        false,
      ),
    )

    const s = store()
    expect(s.issues).toEqual([err])
    expect(s.terminal).toBe('failed')
  })

  it('OperationStarted clears any stale phase label', () => {
    store().begin('validate')
    store().applyFrame({ type: 'PhaseStarted', phase: 'Old phase' })
    store().applyFrame({ type: 'OperationStarted', operation_name: 'validate', env: 'dev' })
    expect(store().phase).toBeNull()
  })

  it('PipelineStarted before any progress frame seeds zeroed counters', () => {
    store().begin('generate')
    store().applyFrame({ type: 'PipelineStarted', pipeline: 'bronze' })
    expect(store().progress).toEqual({ total: 0, done: 0, current: 'bronze' })
  })

  it('PipelineCompleted leaves run state untouched', () => {
    store().begin('generate')
    store().applyFrame({ type: 'progress', total: 2, done: 1, current: 'bronze' })
    const before = store()
    store().applyFrame({
      type: 'PipelineCompleted',
      pipeline: 'bronze',
      duration_s: 1.2,
      files_written: 3,
    })
    const after = store()
    expect(after.progress).toBe(before.progress)
    expect(after.issues).toBe(before.issues)
    expect(after.phase).toBe(before.phase)
  })

  it('an unknown frame type is ignored (forward compatibility)', () => {
    store().begin('validate')
    const before = store()
    store().applyFrame({ type: 'SomethingNew' } as unknown as StreamFrame)
    const after = store()
    expect(after.issues).toBe(before.issues)
    expect(after.terminal).toBeNull()
  })

  it('accumulates info frames in order', () => {
    store().begin('validate')
    store().applyFrame({ type: 'info', code: 'A', message: 'first' })
    store().applyFrame({ type: 'info', code: 'B', message: 'second' })
    expect(store().infoLog).toEqual(['first', 'second'])
  })

  it('a terminal error frame records the frame and the error outcome', () => {
    store().begin('generate')
    const frame: StreamFrame = {
      type: 'error',
      code: 'LHP-IO-001',
      title: 'Disk full',
      details: null,
      suggestions: ['free space'],
      context: {},
      doc_link: null,
    }
    store().applyFrame(frame)

    expect(store().errorFrame).toEqual(frame)
    expect(store().terminal).toBe('error')
  })
})

describe('runStore terminal transitions', () => {
  it('finish() after a clean stream defaults the outcome to success', () => {
    store().begin('validate')
    store().finish()
    expect(store().isRunning).toBe(false)
    expect(store().terminal).toBe('success')
  })

  it('finish() does not clobber an earlier terminal outcome', () => {
    store().begin('generate')
    store().applyFrame({
      type: 'error',
      code: 'X',
      title: 'failed',
      details: null,
      suggestions: [],
      context: {},
      doc_link: null,
    })
    store().finish()
    expect(store().terminal).toBe('error')
    expect(store().isRunning).toBe(false)
  })

  it('fail() coerces a plain Error into a STREAM_ERROR frame', () => {
    store().begin('validate')
    store().fail(new Error('connection reset'))

    const s = store()
    expect(s.errorFrame).toMatchObject({ type: 'error', code: 'STREAM_ERROR', title: 'connection reset' })
    expect(s.terminal).toBe('error')
  })

  it('fail() carries an ApiError’s code and suggestions into the frame', () => {
    store().begin('validate')
    store().fail(
      new ApiError(422, {
        code: 'LHP-CFG-007',
        category: 'config',
        message: 'unknown environment',
        details: '',
        suggestions: ['check substitutions/'],
        context: {},
        http_status: 422,
      }),
    )

    expect(store().errorFrame).toMatchObject({
      type: 'error',
      code: 'LHP-CFG-007',
      title: 'unknown environment',
      suggestions: ['check substitutions/'],
    })
    expect(store().terminal).toBe('error')
  })

  it('fail() stores a server ErrorFrame as-is', () => {
    store().begin('generate')
    const frame: ErrorFrame = {
      type: 'error',
      code: 'LHP-IO-002',
      title: 'write failed',
      details: 'disk',
      suggestions: [],
      context: {},
      doc_link: null,
    }
    store().fail(frame)
    expect(store().errorFrame).toBe(frame)
  })

  it('fail() does not clobber an earlier failed terminal outcome', () => {
    store().begin('validate')
    store().applyFrame(validationCompleted({ bronze: validationResult([issue()], false) }, false))
    expect(store().terminal).toBe('failed')

    store().fail(new Error('stream dropped after the terminal frame'))
    expect(store().terminal).toBe('failed')
  })

  it('reset() returns the store to idle', () => {
    store().begin('generate')
    store().applyFrame({ type: 'PhaseStarted', phase: 'Generating' })
    store().applyFrame({ type: 'info', code: 'I', message: 'm' })
    store().reset()

    const s = store()
    expect(s.runKind).toBeNull()
    expect(s.isRunning).toBe(false)
    expect(s.phase).toBeNull()
    expect(s.progress).toBeNull()
    expect(s.issues).toEqual([])
    expect(s.terminal).toBeNull()
    expect(s.errorFrame).toBeNull()
    expect(s.infoLog).toEqual([])
  })
})

describe('runStore.setSyntheticSyntaxIssue', () => {
  it('adds, replaces, and clears a YAML-SYNTAX issue per file', () => {
    store().begin('validate')
    store().setSyntheticSyntaxIssue('pipelines/a.yaml', { line: 3, column: 5, message: 'bad indent' })

    expect(store().issues).toHaveLength(1)
    expect(store().issues[0]).toMatchObject({
      code: 'YAML-SYNTAX',
      severity: 'error',
      file_path: 'pipelines/a.yaml',
      title: 'bad indent',
      context: { line: 3, column: 5 },
    })

    // Replaces (not appends) for the same file.
    store().setSyntheticSyntaxIssue('pipelines/a.yaml', { line: 9, column: 1, message: 'still bad' })
    expect(store().issues).toHaveLength(1)
    expect(store().issues[0].title).toBe('still bad')

    // Clears on null.
    store().setSyntheticSyntaxIssue('pipelines/a.yaml', null)
    expect(store().issues).toHaveLength(0)
  })

  it('scopes replace/clear to the file, leaving other issues intact', () => {
    store().begin('validate')
    store().applyFrame({
      type: 'WarningEmitted',
      message: 'unrelated warning',
      code: 'W',
      category: 'c',
      file: 'pipelines/other.yaml',
      flowgroup: null,
    })
    store().setSyntheticSyntaxIssue('pipelines/a.yaml', { line: 1, column: 1, message: 'bad' })
    store().setSyntheticSyntaxIssue('pipelines/b.yaml', { line: 2, column: 2, message: 'worse' })
    expect(store().issues).toHaveLength(3)

    // Clearing a.yaml leaves the warning and b.yaml's issue alone.
    store().setSyntheticSyntaxIssue('pipelines/a.yaml', null)
    expect(store().issues.map((i) => i.code)).toEqual(['W', 'YAML-SYNTAX'])
    expect(store().issues[1].file_path).toBe('pipelines/b.yaml')
  })

  it('clearing when no synthetic issue exists keeps the same issues reference', () => {
    store().begin('validate')
    store().applyFrame({
      type: 'WarningEmitted',
      message: 'w',
      code: 'W',
      category: 'c',
      file: null,
      flowgroup: null,
    })
    const before = store().issues
    store().setSyntheticSyntaxIssue('pipelines/untouched.yaml', null)
    expect(store().issues).toBe(before)
  })
})
