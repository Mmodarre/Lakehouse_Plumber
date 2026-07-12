import { act, renderHook } from '@testing-library/react'
import { beforeEach, describe, expect, it, vi } from 'vitest'

// Back the run store with a real zustand store so selectors re-render and
// subscribe() fires, plus a startValidate that mirrors begin() (reset + run).
vi.mock('@/store/runStore', async () => {
  const { create } = await import('zustand')
  const useRunStore = create(() => ({
    isRunning: false,
    runKind: null as string | null,
    terminal: null as string | null,
    errorFrame: null as unknown,
    issues: [] as unknown[],
  }))
  const startValidate = vi.fn(() => {
    useRunStore.setState({
      isRunning: true,
      runKind: 'validate',
      terminal: null,
      errorFrame: null,
      issues: [],
    })
  })
  return {
    useRunStore,
    useRunController: () => ({ startValidate, startGenerate: vi.fn(), abort: vi.fn() }),
  }
})

import { useRunStore } from '@/store/runStore'
import { useDesignerValidation } from '../useDesignerValidation'
import type { ValidationIssue } from '@/types/api'

function issue(p: Partial<ValidationIssue>): ValidationIssue {
  return {
    code: 'LHP-000',
    category: 'general',
    severity: 'error',
    title: '',
    details: null,
    pipeline_name: 'P_A',
    flowgroup_name: 'FG_A',
    file_path: null,
    suggestions: [],
    context: {},
    doc_link: null,
    ...p,
  }
}

const NODES = [{ id: 'load_a', name: 'load_a' }]

/** Complete the currently-running run: isRunning true → false, with issues. */
function completeRun(issues: ValidationIssue[]) {
  useRunStore.setState({ isRunning: false, terminal: 'failed', errorFrame: null, issues })
}

/** A foreign run (Header / useWorkspaceSave auto-validate) that resets and
 * refills the shared store with another pipeline's issues. */
function foreignRun(issues: ValidationIssue[]) {
  useRunStore.setState({ isRunning: true, runKind: 'validate', issues: [] })
  useRunStore.setState({ isRunning: false, terminal: 'failed', issues })
}

beforeEach(() => {
  vi.clearAllMocks()
  useRunStore.setState({
    isRunning: false,
    runKind: null,
    terminal: null,
    errorFrame: null,
    issues: [],
  })
})

describe('useDesignerValidation — own-run result isolation (C1 regression)', () => {
  it('keeps its own run verdict + badges when a foreign run overwrites the shared store', () => {
    const { result } = renderHook(() =>
      useDesignerValidation({ pipeline: 'P_A', flowgroup: 'FG_A', content: 'C1', nodes: NODES }),
    )
    expect(result.current.status).toBe('idle')

    act(() => {
      result.current.run()
    })
    expect(result.current.status).toBe('running')

    // Our run completes: one error mapped to load_a.
    act(() => {
      completeRun([issue({ severity: 'error', title: "Action 'load_a' has no target" })])
    })
    expect(result.current.status).toBe('done')
    expect(result.current.errorCount).toBe(1)
    expect(result.current.perNode.get('load_a')).toEqual({ errors: 1, warnings: 0 })

    // Foreign run (e.g. useWorkspaceSave auto-validate of P_B) overwrites the bus.
    act(() => {
      foreignRun([issue({ pipeline_name: 'P_B', flowgroup_name: 'FG_B', title: 'other broke' })])
    })

    // Content unchanged → the designer must STILL show its own verdict.
    expect(result.current.status).toBe('done')
    expect(result.current.errorCount).toBe(1)
    expect(result.current.perNode.get('load_a')).toEqual({ errors: 1, warnings: 0 })
  })

  it('invalidates the verdict when the flowgroup content changes', () => {
    const { result, rerender } = renderHook((props) => useDesignerValidation(props), {
      initialProps: { pipeline: 'P_A', flowgroup: 'FG_A', content: 'C1', nodes: NODES },
    })
    act(() => {
      result.current.run()
    })
    act(() => {
      completeRun([issue({ title: 'load_a has no target' })])
    })
    expect(result.current.status).toBe('done')
    expect(result.current.errorCount).toBe(1)

    rerender({ pipeline: 'P_A', flowgroup: 'FG_A', content: 'C2-edited', nodes: NODES })
    expect(result.current.status).toBe('idle')
    expect(result.current.errorCount).toBe(0)
    expect(result.current.perNode.size).toBe(0)
  })

  it('re-run replaces the previous result', () => {
    const { result } = renderHook(() =>
      useDesignerValidation({ pipeline: 'P_A', flowgroup: 'FG_A', content: 'C1', nodes: NODES }),
    )
    act(() => {
      result.current.run()
    })
    act(() => {
      completeRun([
        issue({ severity: 'error', title: 'load_a has no target' }),
        issue({ severity: 'warning', title: 'load_a is slow' }),
      ])
    })
    expect(result.current.errorCount).toBe(1)
    expect(result.current.warningCount).toBe(1)

    // Re-validate; this time clean.
    act(() => {
      result.current.run()
    })
    expect(result.current.status).toBe('running')
    act(() => {
      completeRun([])
    })
    expect(result.current.status).toBe('done')
    expect(result.current.errorCount).toBe(0)
    expect(result.current.warningCount).toBe(0)
    expect(result.current.perNode.size).toBe(0)
  })
})
