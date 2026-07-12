import { describe, expect, it, vi } from 'vitest'
import { act, renderHook } from '@testing-library/react'
import type { Edge, Node } from '@xyflow/react'
import type { DesignerEdgeMeta } from '../designerGraph'
import { DRAWON_CLASS } from '../designerEdges'
import {
  useDesignerCanvasWiring,
  useNewEdgeKeys,
  type DesignerCanvasWiringArgs,
} from '../useDesignerCanvasWiring'

// The draw-on is the signature motion; these tests are its safety net. They
// must FAIL if the new-edge key logic breaks (e.g. a lost separator or a
// re-introduced split-into-characters bug) — otherwise the animation could go
// silently dead while the rest of the suite stays green.

describe('useNewEdgeKeys — new-edge detection (draws on once)', () => {
  it('seeds the first populated render without animating', () => {
    const { result } = renderHook(({ keys }) => useNewEdgeKeys(keys), {
      initialProps: { keys: ['k1', 'k2'] },
    })
    expect(result.current.size).toBe(0)
  })

  it('marks exactly the whole key added on a later render', () => {
    const { result, rerender } = renderHook(({ keys }) => useNewEdgeKeys(keys), {
      initialProps: { keys: ['k1', 'k2'] },
    })
    rerender({ keys: ['k1', 'k2', 'k3'] })
    // The full added key — not its characters, which is what a broken
    // separator round-trip would (silently) put here instead.
    expect(result.current).toEqual(new Set(['k3']))
  })

  it('does not re-animate on a relayout with identical content (new array identity)', () => {
    const { result, rerender } = renderHook(({ keys }) => useNewEdgeKeys(keys), {
      initialProps: { keys: ['k1', 'k2'] },
    })
    rerender({ keys: ['k1', 'k2'].slice() })
    expect(result.current.size).toBe(0)
  })

  it('clears the animating set after the draw-on window', () => {
    vi.useFakeTimers()
    try {
      const { result, rerender } = renderHook(({ keys }) => useNewEdgeKeys(keys), {
        initialProps: { keys: ['k1'] },
      })
      rerender({ keys: ['k1', 'k2'] })
      expect(result.current.has('k2')).toBe(true)
      act(() => {
        vi.advanceTimersByTime(600)
      })
      expect(result.current.size).toBe(0)
    } finally {
      vi.useRealTimers()
    }
  })
})

// End-to-end through the real hook: exercises stableKeysFor (which uses the
// field separator), the new-edge detection, and decorate+gate together — the
// draw-on class must land on the genuinely new edge and nowhere else.
describe('useDesignerCanvasWiring — draw-on reaches only the new edge', () => {
  function node(id: string): Node {
    return { id, type: 'load', position: { x: 0, y: 0 }, data: { nodeType: 'load' } } as unknown as Node
  }
  function edge(id: string, source: string, target: string): Edge {
    return { id, source, target, type: 'dependency', data: { edgeType: 'internal' } }
  }
  function args(rfNodes: Node[], rfEdges: Edge[], edgeMeta: DesignerEdgeMeta[]): DesignerCanvasWiringArgs {
    return {
      rfNodes,
      rfEdges,
      edgeMeta,
      selectedId: null,
      setSelectedId: vi.fn(),
      inspectorRef: { current: null },
      isLayouting: false,
      fitView: vi.fn(),
    }
  }

  function drawOnOf(displayEdges: Edge[], source: string, target: string): string {
    const e = displayEdges.find((x) => x.source === source && x.target === target)
    return String(e?.data?.drawOnClass ?? 'MISSING')
  }

  it('does not animate the initial edges, then draws on a newly added edge only', () => {
    const { result, rerender } = renderHook((p: DesignerCanvasWiringArgs) => useDesignerCanvasWiring(p), {
      initialProps: args([node('a'), node('b')], [edge('e0', 'a', 'b')], [{ viewName: 'v_ab', kind: 'data' }]),
    })
    // First populated render: nothing animates (not a whole-graph re-animation).
    expect(drawOnOf(result.current.displayEdges, 'a', 'b')).toBe('')

    rerender(
      args(
        [node('a'), node('b'), node('c')],
        [edge('e0', 'a', 'b'), edge('e1', 'b', 'c')],
        [
          { viewName: 'v_ab', kind: 'data' },
          { viewName: 'v_bc', kind: 'data' },
        ],
      ),
    )
    expect(drawOnOf(result.current.displayEdges, 'b', 'c')).toBe(DRAWON_CLASS)
    expect(drawOnOf(result.current.displayEdges, 'a', 'b')).toBe('')
  })
})
