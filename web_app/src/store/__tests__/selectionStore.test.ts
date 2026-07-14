import { beforeEach, describe, expect, it } from 'vitest'
import { act, renderHook } from '@testing-library/react'
import { useSelectionStore, useTabSelection } from '../selectionStore'
import type { InspectorSelection } from '../selectionStore'

function source(name: string): InspectorSelection {
  return {
    mode: 'source',
    actionId: name,
    action: {
      name,
      kind: 'load',
      subType: 'cloudfiles',
      sources: [],
      dependsOn: [],
      raw: { name, type: 'load' },
      index: 0,
    },
  }
}

beforeEach(() => {
  useSelectionStore.setState({ byTab: {} })
})

describe('selectionStore', () => {
  it('sets + reads a selection keyed by tab id', () => {
    useSelectionStore.getState().setSelection('t1', source('load_a'))
    expect(useSelectionStore.getState().getSelection('t1')).toMatchObject({
      mode: 'source',
      actionId: 'load_a',
    })
    expect(useSelectionStore.getState().getSelection('t2')).toBeNull()
  })

  it('setSelection(null) clears just that tab', () => {
    useSelectionStore.getState().setSelection('t1', source('a'))
    useSelectionStore.getState().setSelection('t2', source('b'))
    useSelectionStore.getState().setSelection('t1', null)
    expect(useSelectionStore.getState().getSelection('t1')).toBeNull()
    expect(useSelectionStore.getState().getSelection('t2')).toMatchObject({ actionId: 'b' })
  })

  it('clearSelection removes only the named tab', () => {
    useSelectionStore.getState().setSelection('t1', source('a'))
    useSelectionStore.getState().setSelection('t2', source('b'))
    useSelectionStore.getState().clearSelection('t1')
    expect(useSelectionStore.getState().getSelection('t1')).toBeNull()
    expect(useSelectionStore.getState().getSelection('t2')).toMatchObject({ actionId: 'b' })
  })

  it('preserves an external-mode selection verbatim', () => {
    const ext: InspectorSelection = { mode: 'external', label: 'other.view', consumers: ['x'] }
    useSelectionStore.getState().setSelection('t1', ext)
    expect(useSelectionStore.getState().getSelection('t1')).toEqual(ext)
  })

  it('useTabSelection tracks the store for a tab (null id ⇒ null)', () => {
    const { result, rerender } = renderHook(({ id }) => useTabSelection(id), {
      initialProps: { id: 't1' as string | null },
    })
    expect(result.current).toBeNull()

    act(() => useSelectionStore.getState().setSelection('t1', source('a')))
    expect(result.current).toMatchObject({ actionId: 'a' })

    rerender({ id: null })
    expect(result.current).toBeNull()
  })
})
