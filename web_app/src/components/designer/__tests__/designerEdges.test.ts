import { describe, expect, it, vi } from 'vitest'
import type { Edge } from '@xyflow/react'
import type { DesignerEdgeMeta } from '../designerGraph'
import {
  DRAWON_CLASS,
  decorateDesignerEdges,
  diffNewEdgeKeys,
  drawOnEdgeClass,
  edgeStableKey,
  stableKeysFor,
} from '../designerEdges'

describe('edgeStableKey', () => {
  it('separates fields so different groupings never collide', () => {
    // Without a field separator, ('a','bc') and ('ab','c') would both be "abc".
    expect(edgeStableKey('a', 'bc', 'd', 'data')).not.toBe(edgeStableKey('ab', 'c', 'd', 'data'))
  })

  it('distinguishes data from depends_on for the same endpoints', () => {
    expect(edgeStableKey('a', 'b', 'v', 'data')).not.toBe(edgeStableKey('a', 'b', 'v', 'depends_on'))
  })
})

describe('stableKeysFor', () => {
  const rfEdges: Edge[] = [
    { id: 'e-0', source: 'a', target: 'b', type: 'dependency' },
    { id: 'e-1', source: 'b', target: 'c', type: 'dependency' },
  ]
  const meta: DesignerEdgeMeta[] = [
    { viewName: 'v_a', kind: 'data' },
    { viewName: 'v_b', kind: 'data' },
  ]

  it('produces one key per edge, aligned by index', () => {
    expect(stableKeysFor(rfEdges, meta)).toEqual([
      edgeStableKey('a', 'b', 'v_a', 'data'),
      edgeStableKey('b', 'c', 'v_b', 'data'),
    ])
  })

  it('returns [] on a length mismatch (mid-relayout render)', () => {
    expect(stableKeysFor(rfEdges, [])).toEqual([])
  })
})

describe('diffNewEdgeKeys', () => {
  it('returns only keys absent from the seen set', () => {
    const seen = new Set(['k1', 'k2'])
    expect([...diffNewEdgeKeys(seen, ['k2', 'k3'])]).toEqual(['k3'])
  })

  it('is empty when nothing is new', () => {
    expect(diffNewEdgeKeys(new Set(['k1']), ['k1']).size).toBe(0)
  })
})

describe('drawOnEdgeClass — the reduced-motion gate', () => {
  it('animates a new edge only when motion is allowed', () => {
    expect(drawOnEdgeClass(true, false)).toBe(DRAWON_CLASS)
  })

  it('never animates when the user prefers reduced motion', () => {
    expect(drawOnEdgeClass(true, true)).toBe('')
  })

  it('never animates an edge that is not new', () => {
    expect(drawOnEdgeClass(false, false)).toBe('')
  })
})

describe('decorateDesignerEdges', () => {
  const rfEdges: Edge[] = [
    { id: 'e-0', source: 'load_orders', target: 'clean', type: 'dependency', data: { edgeType: 'internal' } },
  ]
  const meta: DesignerEdgeMeta[] = [{ viewName: 'v_orders', kind: 'data' }]

  function decorate(over: Partial<Parameters<typeof decorateDesignerEdges>[2]> = {}) {
    return decorateDesignerEdges(rfEdges, meta, {
      onSelectView: vi.fn(),
      kindByNode: new Map([['load_orders', 'load']]),
      newEdgeKeys: new Set(),
      prefersReducedMotion: false,
      ...over,
    })
  }

  it('zips the named-pipe facts onto edge.data', () => {
    const onSelectView = vi.fn()
    const [edge] = decorate({ onSelectView })
    expect(edge.data).toMatchObject({
      edgeType: 'internal',
      viewName: 'v_orders',
      designerKind: 'data',
      producerKind: 'load',
      drawOnClass: '',
    })
    expect(edge.data?.onSelectView).toBe(onSelectView)
  })

  it('marks a new edge with the draw-on class (motion allowed)', () => {
    const key = edgeStableKey('load_orders', 'clean', 'v_orders', 'data')
    const [edge] = decorate({ newEdgeKeys: new Set([key]) })
    expect(edge.data?.drawOnClass).toBe(DRAWON_CLASS)
  })

  it('does not add the draw-on class for a new edge under reduced motion', () => {
    const key = edgeStableKey('load_orders', 'clean', 'v_orders', 'data')
    const [edge] = decorate({ newEdgeKeys: new Set([key]), prefersReducedMotion: true })
    expect(edge.data?.drawOnClass).toBe('')
  })

  it('returns the input unchanged on a length mismatch', () => {
    expect(
      decorateDesignerEdges(rfEdges, [], {
        onSelectView: vi.fn(),
        kindByNode: new Map(),
        newEdgeKeys: new Set(),
        prefersReducedMotion: false,
      }),
    ).toBe(rfEdges)
  })
})
