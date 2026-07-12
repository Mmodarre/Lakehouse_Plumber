import { describe, expect, it, vi } from 'vitest'
import { renderHook } from '@testing-library/react'
import type { KeyboardEvent } from 'react'
import type { Edge, Node } from '@xyflow/react'
import { useDesignerCanvasWiring, type DesignerCanvasWiringArgs } from '../useDesignerCanvasWiring'

function node(id: string, x: number, y: number, nodeType = 'load'): Node {
  return {
    id,
    type: 'load',
    position: { x, y },
    data: { nodeType },
  } as unknown as Node
}

function keyEvent(key: string, tagName = 'DIV'): KeyboardEvent<HTMLDivElement> {
  const target =
    tagName === 'INPUT' ? document.createElement('input') : document.createElement('div')
  return {
    key,
    target,
    preventDefault: vi.fn(),
  } as unknown as KeyboardEvent<HTMLDivElement>
}

function wire(over: Partial<DesignerCanvasWiringArgs> = {}) {
  const setSelectedId = vi.fn()
  const focus = vi.fn()
  const args: DesignerCanvasWiringArgs = {
    rfNodes: [node('a', 0, 0), node('b', 100, 0, 'transform'), node('c', 0, 100, 'write')],
    rfEdges: [] as Edge[],
    edgeMeta: [],
    selectedId: 'a',
    setSelectedId,
    inspectorRef: { current: { focus } as unknown as HTMLElement },
    isLayouting: false,
    fitView: vi.fn(),
    ...over,
  }
  const { result } = renderHook((p: DesignerCanvasWiringArgs) => useDesignerCanvasWiring(p), {
    initialProps: args,
  })
  return { result, setSelectedId, focus }
}

describe('useDesignerCanvasWiring — keyboard navigation', () => {
  it('exposes a keyboard-navigable canvas region', () => {
    const { result } = wire()
    expect(result.current.canvasProps.tabIndex).toBe(0)
    expect(result.current.canvasProps.role).toBe('group')
    expect(result.current.canvasProps['aria-keyshortcuts']).toContain('Enter')
  })

  it('moves the selection to the nearest node in the arrow direction', () => {
    const { result, setSelectedId } = wire({ selectedId: 'a' })
    result.current.canvasProps.onKeyDown(keyEvent('ArrowRight'))
    expect(setSelectedId).toHaveBeenCalledWith('b')

    setSelectedId.mockClear()
    result.current.canvasProps.onKeyDown(keyEvent('ArrowDown'))
    expect(setSelectedId).toHaveBeenCalledWith('c')
  })

  it('opens (focuses) the inspector on Enter when a node is selected', () => {
    const { result, focus } = wire({ selectedId: 'a' })
    result.current.canvasProps.onKeyDown(keyEvent('Enter'))
    expect(focus).toHaveBeenCalledOnce()
  })

  it('does nothing on Enter with no selection', () => {
    const { result, focus } = wire({ selectedId: null })
    result.current.canvasProps.onKeyDown(keyEvent('Enter'))
    expect(focus).not.toHaveBeenCalled()
  })

  it('clears the selection on Escape', () => {
    const { result, setSelectedId } = wire({ selectedId: 'a' })
    result.current.canvasProps.onKeyDown(keyEvent('Escape'))
    expect(setSelectedId).toHaveBeenCalledWith(null)
  })

  it('never hijacks keys typed into a form control', () => {
    const { result, setSelectedId } = wire({ selectedId: 'a' })
    result.current.canvasProps.onKeyDown(keyEvent('ArrowRight', 'INPUT'))
    expect(setSelectedId).not.toHaveBeenCalled()
  })
})
