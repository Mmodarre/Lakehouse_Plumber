import { describe, expect, it, vi, beforeEach } from 'vitest'
import { createRef, useEffect, useRef } from 'react'
import { render } from '@testing-library/react'

// Monaco itself cannot run under jsdom. The mock below replaces
// @monaco-editor/react with a fake editor that faithfully mimics the ONE
// Monaco behaviour these tests depend on: `editor.setValue` fires
// onDidChangeModelContent SYNCHRONOUSLY (the root cause of the
// spurious-dirty bug the wrapper's suppression fixes).
const h = vi.hoisted(() => {
  interface FakeEditor {
    getValue: () => string
    setValue: (v: string) => void
    onDidChangeModelContent: (cb: () => void) => { dispose: () => void }
    addCommand: () => null
    focus: () => void
    getModel: () => object
    /** Test helper: simulate a user keystroke (content change event). */
    __type: (v: string) => void
  }
  const state: { editor: FakeEditor | null } = { editor: null }
  function createFakeEditor(initial: string): FakeEditor {
    let value = initial
    const listeners: Array<() => void> = []
    const fire = () => listeners.forEach((l) => l())
    return {
      getValue: () => value,
      setValue: (v: string) => {
        value = v
        fire() // synchronous, exactly like real Monaco
      },
      onDidChangeModelContent: (cb: () => void) => {
        listeners.push(cb)
        return { dispose: () => {} }
      },
      addCommand: () => null,
      focus: () => {},
      getModel: () => ({}),
      __type: (v: string) => {
        value = v
        fire()
      },
    }
  }
  return { state, createFakeEditor }
})

vi.mock('@monaco-editor/react', () => ({
  default: function FakeMonaco({
    defaultValue,
    onMount,
  }: {
    defaultValue?: string
    onMount?: (ed: unknown, monaco: unknown) => void
  }) {
    const mounted = useRef(false)
    useEffect(() => {
      if (mounted.current) return
      mounted.current = true
      h.state.editor = h.createFakeEditor(defaultValue ?? '')
      onMount?.(h.state.editor, {
        KeyMod: { CtrlCmd: 0 },
        KeyCode: { KeyS: 0 },
        MarkerSeverity: { Error: 8 },
        editor: { setModelMarkers: vi.fn() },
      })
    }, [defaultValue, onMount])
    return <div data-testid="fake-monaco" />
  },
}))

vi.mock('../../../lib/monaco-setup', () => ({
  monacoThemeFor: () => 'lhp-light',
  setupMonacoYaml: () => Promise.resolve(),
}))

import MonacoEditorWrapper from '../MonacoEditorWrapper'
import type { MonacoEditorHandle } from '../MonacoEditorWrapper'
import { useWorkspaceStore } from '../../../store/workspaceStore'

describe('MonacoEditorWrapper programmatic setValue (Fix 1)', () => {
  beforeEach(() => {
    h.state.editor = null
    localStorage.clear()
    useWorkspaceStore.setState({ buffers: [], activePath: null })
  })

  it('never emits a dirty signal for a programmatic setValue (sabotage check)', () => {
    const onDirtyChange = vi.fn()
    const ref = createRef<MonacoEditorHandle>()
    render(
      <MonacoEditorWrapper ref={ref} path="a.yaml" content="orig" onDirtyChange={onDirtyChange} />,
    )
    expect(h.state.editor).not.toBeNull()

    ref.current!.setValue('reloaded from disk')

    // The change event fired synchronously inside setValue; the wrapper must
    // have suppressed it — a single dirty:true here re-marks the store buffer
    // dirty and cancels its own self-heal (the original blocker).
    const trueCalls = onDirtyChange.mock.calls.filter(([d]) => d === true)
    expect(trueCalls).toHaveLength(0)
    expect(onDirtyChange).toHaveBeenCalledWith(false)
    expect(ref.current!.getValue()).toBe('reloaded from disk')
  })

  it('still emits dirty for real user edits', () => {
    const onDirtyChange = vi.fn()
    const ref = createRef<MonacoEditorHandle>()
    render(
      <MonacoEditorWrapper ref={ref} path="a.yaml" content="orig" onDirtyChange={onDirtyChange} />,
    )
    h.state.editor!.__type('user typed this')
    expect(onDirtyChange).toHaveBeenCalledWith(true)
  })

  it('leaves the store buffer clean after a programmatic reload (end-to-end wiring)', () => {
    const path = 'pipelines/raw/orders.yaml'
    const store = useWorkspaceStore.getState()
    store.openBuffer(path, { content: 'orig', etag: 'e1', exists: true })

    // Wire onDirtyChange exactly as WorkspaceEditor.handleDirtyChange does.
    const onDirtyChange = (dirty: boolean) => {
      useWorkspaceStore.getState().setDirty(path, dirty)
    }
    const ref = createRef<MonacoEditorHandle>()
    render(
      <MonacoEditorWrapper ref={ref} path={path} content="orig" onDirtyChange={onDirtyChange} />,
    )

    // User types → dirty; then a programmatic reload (take-theirs / discard /
    // keep-mine success) must end with the buffer CLEAN.
    h.state.editor!.__type('user edit')
    expect(useWorkspaceStore.getState().buffers[0].isDirty).toBe(true)

    useWorkspaceStore.getState().replaceBuffer(path, 'disk version', 'e2')
    ref.current!.setValue('disk version')

    const buf = useWorkspaceStore.getState().buffers[0]
    expect(buf.isDirty).toBe(false)
    expect(buf.content).toBe('disk version')
  })
})
