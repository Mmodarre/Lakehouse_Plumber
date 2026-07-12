import { describe, expect, it } from 'vitest'
import type { Edge } from '@xyflow/react'
import { deriveGraph, parseFlowgroupFile, selectFlowgroup } from '../../../lib/flowgroup-doc'
import type { FlowgroupActionSummary } from '../../../types/api'
import {
  actionConfigRows,
  attachEdgeMeta,
  resolvedDesignerGraph,
  resolvedSublabel,
  sourceSublabel,
  toDesignerGraph,
} from '../designerGraph'

// ── source view mapping ──────────────────────────────────────

const FIXTURE = `pipeline: bronze
flowgroup: orders
actions:
  - name: load_orders
    type: load
    source:
      type: cloudfiles
      path: /mnt/raw/orders
      format: json
    target: v_orders_raw
  - name: clean_orders
    type: transform
    transform_type: sql
    source: v_orders_raw
    target: v_orders_clean
    depends_on:
      - audit_view
  - name: write_orders
    type: write
    source: v_orders_clean
    depends_on:
      - v_orders_raw
    write_target:
      type: streaming_table
      database: catalog.bronze
      table: orders
  - name: mystery
`

function sourceGraph() {
  const file = parseFlowgroupFile(FIXTURE)
  const doc = selectFlowgroup(file, 'orders')
  if (doc === undefined) throw new Error('fixture flowgroup missing')
  return deriveGraph(doc)
}

describe('toDesignerGraph', () => {
  it('maps actions to kind-typed nodes and externals to muted external nodes', () => {
    const data = toDesignerGraph(sourceGraph(), 'bronze', 'orders')

    expect(data.nodes.map((n) => [n.id, n.type])).toEqual([
      ['load_orders', 'load'],
      ['clean_orders', 'transform'],
      ['write_orders', 'write'],
      ['mystery', 'action'], // unknown kind → generic action card
      ['ext:audit_view', 'external'],
    ])
    expect(data.nodes.map((n) => n.metadata?.sublabel)).toEqual([
      'Load (cloudfiles)',
      'Transform (sql)',
      'Streaming table',
      'Unknown type',
      undefined,
    ])
    // actionIndex pairs each node back to listActions for the inspector.
    expect(data.nodes.slice(0, 4).map((n) => n.metadata?.actionIndex)).toEqual([0, 1, 2, 3])
    expect(data.nodes.every((n) => n.pipeline === 'bronze' && n.flowgroup === 'orders')).toBe(
      true,
    )
  })

  it('classifies edges (external / depends_on / data) and aligns edgeMeta by index', () => {
    const data = toDesignerGraph(sourceGraph(), 'bronze', 'orders')

    expect(data.edges).toEqual([
      { source: 'load_orders', target: 'clean_orders', type: 'internal' },
      { source: 'ext:audit_view', target: 'clean_orders', type: 'external' },
      { source: 'clean_orders', target: 'write_orders', type: 'internal' },
      // depends_on onto an in-flowgroup producer renders dashed
      { source: 'load_orders', target: 'write_orders', type: 'cross_flowgroup' },
    ])
    expect(data.edgeMeta).toEqual([
      { viewName: 'v_orders_raw', kind: 'data' },
      { viewName: 'audit_view', kind: 'depends_on' },
      { viewName: 'v_orders_clean', kind: 'data' },
      { viewName: 'v_orders_raw', kind: 'depends_on' },
    ])
  })

  it('labels unnamed actions by position', () => {
    const file = parseFlowgroupFile(
      'pipeline: p\nflowgroup: f\nactions:\n  - type: load\n    target: v\n',
    )
    const doc = selectFlowgroup(file, 'f')
    if (doc === undefined) throw new Error('fixture flowgroup missing')
    const data = toDesignerGraph(deriveGraph(doc), 'p', 'f')
    expect(data.nodes[0].label).toBe('(action 1)')
  })
})

describe('sourceSublabel', () => {
  it('words each kind like the dashboard action nodes', () => {
    expect(sourceSublabel('load', 'sql')).toBe('Load (sql)')
    expect(sourceSublabel('transform', 'python')).toBe('Transform (python)')
    expect(sourceSublabel('write', 'materialized_view')).toBe('Materialized view')
    expect(sourceSublabel('write', 'sink')).toBe('Sink')
    expect(sourceSublabel('test', 'row_count')).toBe('Test (row_count)')
    expect(sourceSublabel('unknown', 'anything')).toBe('Unknown type')
  })

  it('appends non-standard write modes only', () => {
    expect(sourceSublabel('write', 'streaming_table', 'standard')).toBe('Streaming table')
    expect(sourceSublabel('write', 'streaming_table', 'cdc')).toBe('Streaming table · cdc')
  })
})

// ── resolved view mapping ────────────────────────────────────

function summary(partial: Partial<FlowgroupActionSummary> & { name: string; type: string }) {
  return partial as FlowgroupActionSummary
}

describe('resolvedDesignerGraph', () => {
  it('renders actions as a node list with no edges (DTO has no source fields)', () => {
    const data = resolvedDesignerGraph(
      [
        summary({ name: 'a', type: 'load' }),
        summary({ name: 'a', type: 'transform', transform_type: 'python' }),
        summary({ name: 'w', type: 'write', write_mode: 'cdc', scd_type: 2 }),
        summary({ name: 'x', type: 'foo' }),
      ],
      'bronze',
      'orders',
    )

    expect(data.edges).toEqual([])
    expect(data.edgeMeta).toEqual([])
    // duplicate names (possible after template expansion) get suffixed ids
    expect(data.nodes.map((n) => n.id)).toEqual(['a', 'a#2', 'w', 'x'])
    expect(data.nodes.map((n) => n.type)).toEqual(['load', 'transform', 'write', 'action'])
    expect(data.nodes.map((n) => n.metadata?.actionIndex)).toEqual([0, 1, 2, 3])
  })
})

describe('resolvedSublabel', () => {
  it('never claims a write target type the DTO does not carry', () => {
    expect(resolvedSublabel(summary({ name: 'w', type: 'write' }))).toBe('Write')
    expect(
      resolvedSublabel(summary({ name: 'w', type: 'write', write_mode: 'cdc', scd_type: 2 })),
    ).toBe('Write · cdc · SCD 2')
  })

  it('uses sub-type fields where the DTO has them', () => {
    expect(resolvedSublabel(summary({ name: 't', type: 'transform' }))).toBe('Transform')
    expect(
      resolvedSublabel(summary({ name: 't', type: 'transform', transform_type: 'sql' })),
    ).toBe('Transform (sql)')
    expect(resolvedSublabel(summary({ name: 't', type: 'test', test_type: 'uniqueness' }))).toBe(
      'Test (uniqueness)',
    )
    expect(resolvedSublabel(summary({ name: 'l', type: 'load' }))).toBe('Load')
    expect(resolvedSublabel(summary({ name: 'z', type: 'foo' }))).toBe('Foo')
  })
})

// ── edge meta attachment ─────────────────────────────────────

describe('attachEdgeMeta', () => {
  const rfEdges: Edge[] = [
    { id: 'e-0', source: 'a', target: 'b', type: 'dependency', data: { edgeType: 'internal' } },
  ]

  it('zips viewName/designerKind onto the layouted edges by index', () => {
    const result = attachEdgeMeta(rfEdges, [{ viewName: 'v_a', kind: 'data' }])
    expect(result[0].data).toEqual({
      edgeType: 'internal',
      viewName: 'v_a',
      designerKind: 'data',
    })
  })

  it('returns the input unchanged on a length mismatch (mid-relayout render)', () => {
    expect(attachEdgeMeta(rfEdges, [])).toBe(rfEdges)
  })
})

// ── inspector key-config rows ────────────────────────────────

describe('actionConfigRows', () => {
  it('skips fixed identity fields and flattens nested mappings to dotted keys', () => {
    const rows = actionConfigRows({
      name: 'load_orders',
      type: 'load',
      target: 'v_orders_raw',
      description: 'shown separately',
      depends_on: ['x'],
      readMode: 'stream',
      source: { type: 'cloudfiles', path: '/mnt/raw', format: 'json' },
    })
    expect(rows).toEqual([
      { key: 'readMode', value: 'stream' },
      { key: 'source.type', value: 'cloudfiles' },
      { key: 'source.path', value: '/mnt/raw' },
      { key: 'source.format', value: 'json' },
    ])
  })

  it('summarizes lists, deep mappings, and long strings quietly', () => {
    const rows = actionConfigRows({
      columns: ['a', 'b', 'c', 'd', 'e'],
      expectations: [{ name: 'not_null' }, { name: 'unique' }],
      write_target: { cdc_config: { keys: ['id'], sequence_by: { col: 'ts' } } },
      sql: 'x'.repeat(200),
    })
    expect(rows).toEqual([
      { key: 'columns', value: 'a, b, c, … (+2)' },
      { key: 'expectations', value: '2 items' },
      { key: 'write_target.cdc_config.keys', value: 'id' },
      { key: 'write_target.cdc_config.sequence_by', value: '1 field' },
      { key: 'sql', value: `${'x'.repeat(79)}…` },
    ])
  })

  it('drops null/undefined and empty arrays', () => {
    expect(actionConfigRows({ a: null, b: undefined, c: [] })).toEqual([])
  })
})
