/**
 * actionSkeleton — minimal valid action objects per (kind, subType).
 *
 * Guarantees: the sub-type discriminator is correct, always-visible required
 * fields are present, mode-gated fields stay out, producers get a derived
 * target, and pre-wiring a downstream action makes an edge appear in
 * deriveGraph.
 */
import { describe, expect, it } from 'vitest'

import {
  addAction,
  deriveGraph,
  parseFlowgroupFile,
  selectFlowgroup,
} from '@/lib/flowgroup-doc'
import {
  buildActionSkeleton,
  buildInsertion,
  prewireSource,
  sourceFieldShape,
  uniqueActionName,
} from '../actionSkeleton'

describe('uniqueActionName', () => {
  it('increments past taken names', () => {
    expect(uniqueActionName('cloudfiles', [])).toBe('cloudfiles_1')
    expect(uniqueActionName('cloudfiles', ['cloudfiles_1', 'cloudfiles_2'])).toBe('cloudfiles_3')
  })
})

describe('buildActionSkeleton', () => {
  it('load/cloudfiles: source.type discriminator + required path/format + derived target', () => {
    const a = buildActionSkeleton('load', 'cloudfiles', [])
    expect(a.name).toBe('cloudfiles_1')
    expect(a.type).toBe('load')
    expect(a.source).toEqual({ type: 'cloudfiles', path: '', format: '' })
    expect(a.target).toBe('v_cloudfiles_1')
  })

  it('transform/sql: transform_type discriminator + required source + derived target', () => {
    const a = buildActionSkeleton('transform', 'sql', [])
    expect(a.type).toBe('transform')
    expect(a.transform_type).toBe('sql')
    expect(a.source).toBe('')
    expect(a.target).toBe('v_sql_1')
  })

  it('write/streaming_table: standard-mode required fields only (no cdc_config)', () => {
    const a = buildActionSkeleton('write', 'streaming_table', [])
    expect(a.type).toBe('write')
    expect(a.write_target).toEqual({ type: 'streaming_table', catalog: '', schema: '', table: '' })
    expect(a.source).toBe('')
    // Mode-gated group stays out of a standard-mode skeleton.
    expect(a).not.toHaveProperty('target')
    expect((a.write_target as Record<string, unknown>).cdc_config).toBeUndefined()
  })

  it('test/row_count: test_type discriminator + list source', () => {
    const a = buildActionSkeleton('test', 'row_count', [])
    expect(a.type).toBe('test')
    expect(a.test_type).toBe('row_count')
    expect(a.source).toEqual([])
  })
})

describe('sourceFieldShape', () => {
  it('classifies the primary source ref per sub-type', () => {
    expect(sourceFieldShape('load', 'cloudfiles')).toBeNull()
    expect(sourceFieldShape('transform', 'sql')).toBe('string')
    expect(sourceFieldShape('write', 'streaming_table')).toBe('string')
    expect(sourceFieldShape('test', 'row_count')).toBe('list')
  })
})

describe('prewireSource', () => {
  it('sets a string source for string-shaped sub-types', () => {
    const a = buildActionSkeleton('transform', 'sql', [])
    prewireSource(a, 'transform', 'sql', 'v_up')
    expect(a.source).toBe('v_up')
  })

  it('sets a one-element list for list-shaped sub-types', () => {
    const a = buildActionSkeleton('test', 'row_count', [])
    prewireSource(a, 'test', 'row_count', 'v_up')
    expect(a.source).toEqual(['v_up'])
  })

  it('leaves structured-source loads unwired', () => {
    const a = buildActionSkeleton('load', 'cloudfiles', [])
    prewireSource(a, 'load', 'cloudfiles', 'v_up')
    expect(a.source).toEqual({ type: 'cloudfiles', path: '', format: '' })
  })
})

describe('buildInsertion + deriveGraph', () => {
  it('inserting downstream of a producer wires the edge', () => {
    const src = `pipeline: p
flowgroup: fg
actions:
  - name: load_x
    type: load
    source:
      type: delta
      table: t
    target: v_x
`
    const handle = parseFlowgroupFile(src)
    const doc = selectFlowgroup(handle, 'fg')!
    const action = buildInsertion('transform', 'sql', { existing: ['load_x'], upstreamView: 'v_x' })
    expect(action.source).toBe('v_x')
    addAction(doc, action, 'load_x')

    const graph = deriveGraph(doc)
    const edge = graph.edges.find((e) => e.viewName === 'v_x' && e.to === 'sql_1')
    expect(edge).toBeDefined()
    expect(edge!.from).toBe('load_x')
  })
})
