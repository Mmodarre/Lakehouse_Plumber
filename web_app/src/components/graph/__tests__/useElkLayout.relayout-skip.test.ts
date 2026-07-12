// Relayout-skip contract for useElkLayout: elk.layout() must re-run on any
// structural change (node set, edge endpoints, stage partitions) and must be
// skipped — while node/edge data still refreshes — for structure-preserving
// input churn (new array/Map identities, label-only edits, order shuffles).
import { describe, expect, it, beforeEach, vi } from 'vitest'
import { renderHook, waitFor } from '@testing-library/react'
import type { GraphNode, GraphEdge } from '../../../types/api'
import type { ExternalConnection } from '../../../types/graph'
import { useElkLayout } from '../useElkLayout'

const { layoutSpy } = vi.hoisted(() => ({ layoutSpy: vi.fn() }))

interface MockElkChild extends Record<string, unknown> {
  id: string
}
interface MockElkGraph extends Record<string, unknown> {
  children?: MockElkChild[]
}

vi.mock('elkjs/lib/elk-api.js', () => ({
  default: class MockElk {
    async layout(graph: MockElkGraph): Promise<MockElkGraph> {
      layoutSpy(graph)
      return {
        ...graph,
        children: (graph.children ?? []).map((c, i) => ({
          ...c,
          x: i * 100,
          y: i * 10,
        })),
      }
    }
  },
}))

function node(id: string, overrides: Partial<GraphNode> = {}): GraphNode {
  return {
    id,
    label: id,
    pipeline: 'p1',
    flowgroup: 'fg1',
    stage: 0,
    type: 'flowgroup',
    metadata: {},
    ...overrides,
  }
}

function edge(source: string, target: string, type = 'internal'): GraphEdge {
  return { source, target, type }
}

interface HookProps {
  n: GraphNode[]
  e: GraphEdge[]
  ext?: Map<string, ExternalConnection[]>
}

function renderLayout(initial: HookProps) {
  return renderHook(({ n, e, ext }: HookProps) => useElkLayout(n, e, ext), {
    initialProps: initial,
  })
}

function positionsById(nodes: { id: string; position: { x: number; y: number } }[]) {
  return new Map(nodes.map((n) => [n.id, n.position]))
}

beforeEach(() => {
  layoutSpy.mockClear()
})

describe('useElkLayout relayout skip', () => {
  it('runs ELK once on mount', async () => {
    const { result } = renderLayout({ n: [node('a'), node('b')], e: [edge('a', 'b')] })

    await waitFor(() => expect(result.current.nodes).toHaveLength(2))
    expect(layoutSpy).toHaveBeenCalledTimes(1)
    expect(result.current.edges).toHaveLength(1)
  })

  it('skips ELK for identity-only churn but refreshes node data and badges', async () => {
    const { result, rerender } = renderLayout({
      n: [node('a'), node('b')],
      e: [edge('a', 'b')],
    })
    await waitFor(() => expect(result.current.nodes).toHaveLength(2))
    const firstPositions = positionsById(result.current.nodes)

    // Same structure, new array identities, a renamed label, and a fresh
    // external-connections Map — the FlowgroupMiniGraph fullData pattern.
    const ext = new Map<string, ExternalConnection[]>([
      ['a', [{ direction: 'downstream', targetNodeId: 'x', targetPipeline: 'p2' }]],
    ])
    rerender({ n: [node('a', { label: 'renamed' }), node('b')], e: [edge('a', 'b')], ext })

    await waitFor(() => {
      const a = result.current.nodes.find((n) => n.id === 'a')
      expect(a?.data.label).toBe('renamed')
    })
    expect(layoutSpy).toHaveBeenCalledTimes(1)
    const a = result.current.nodes.find((n) => n.id === 'a')
    expect(a?.data.externalConnections).toEqual(ext.get('a'))
    expect(positionsById(result.current.nodes)).toEqual(firstPositions)
  })

  it('reuses the cached layout when the same structure arrives in a different order', async () => {
    const { result, rerender } = renderLayout({
      n: [node('a'), node('b')],
      e: [edge('a', 'b')],
    })
    await waitFor(() => expect(result.current.nodes).toHaveLength(2))
    const firstPositions = positionsById(result.current.nodes)

    rerender({ n: [node('b'), node('a')], e: [edge('a', 'b')] })

    await waitFor(() => expect(result.current.nodes).toHaveLength(2))
    expect(layoutSpy).toHaveBeenCalledTimes(1)
    expect(positionsById(result.current.nodes)).toEqual(firstPositions)
  })

  it('re-runs ELK when a node is added', async () => {
    const { result, rerender } = renderLayout({
      n: [node('a'), node('b')],
      e: [edge('a', 'b')],
    })
    await waitFor(() => expect(result.current.nodes).toHaveLength(2))

    rerender({ n: [node('a'), node('b'), node('c')], e: [edge('a', 'b'), edge('b', 'c')] })

    await waitFor(() => expect(result.current.nodes).toHaveLength(3))
    expect(layoutSpy).toHaveBeenCalledTimes(2)
  })

  it('re-runs ELK when edge endpoints change with the same node set', async () => {
    const { result, rerender } = renderLayout({
      n: [node('a'), node('b')],
      e: [edge('a', 'b')],
    })
    await waitFor(() => expect(result.current.nodes).toHaveLength(2))

    rerender({ n: [node('a'), node('b')], e: [edge('b', 'a')] })

    await waitFor(() => expect(layoutSpy).toHaveBeenCalledTimes(2))
  })

  it('re-runs ELK when a stage partition changes', async () => {
    const { result, rerender } = renderLayout({
      n: [node('a'), node('b')],
      e: [edge('a', 'b')],
    })
    await waitFor(() => expect(result.current.nodes).toHaveLength(2))

    rerender({ n: [node('a'), node('b', { stage: 1 })], e: [edge('a', 'b')] })

    await waitFor(() => expect(layoutSpy).toHaveBeenCalledTimes(2))
  })

  it('does not re-run ELK when only the edge kind changes', async () => {
    const { result, rerender } = renderLayout({
      n: [node('a'), node('b')],
      e: [edge('a', 'b', 'internal')],
    })
    await waitFor(() => expect(result.current.edges).toHaveLength(1))

    rerender({ n: [node('a'), node('b')], e: [edge('a', 'b', 'cross_pipeline')] })

    await waitFor(() =>
      expect(result.current.edges[0]?.data?.edgeType).toBe('cross_pipeline'),
    )
    expect(layoutSpy).toHaveBeenCalledTimes(1)
  })
})
