/**
 * appendActionSource — the fan-in mutator (Task 9, req 5).
 *
 * Closes the Task-3 stringOrList non-conversion gap: a single-string source
 * becomes a two-element list, a list source appends surgically, a structured
 * (load) source is rejected. The canonical fan-in case — two loads joined by
 * one transform — must derive two inbound data edges.
 */
import { describe, expect, it } from 'vitest'

import {
  appendActionSource,
  deriveGraph,
  listActions,
  parseFlowgroupFile,
  selectFlowgroup,
  serializeFlowgroupFile,
} from '../flowgroup-doc'
import type { FlowgroupDocHandle, FlowgroupFileHandle } from '../flowgroup-doc'

function mustSelect(source: string): { handle: FlowgroupFileHandle; doc: FlowgroupDocHandle } {
  const handle = parseFlowgroupFile(source)
  const doc = selectFlowgroup(handle, 'fg')
  if (doc === undefined) throw new Error('flowgroup fg not found')
  return { handle, doc }
}

const FAN_IN = `pipeline: p
flowgroup: fg
actions:
  - name: load_a
    type: load
    source:
      type: delta
      table: t_a
    target: v_a
  - name: load_b
    type: load
    source:
      type: delta
      table: t_b
    target: v_b
  - name: join
    type: transform
    transform_type: sql
    source: v_a
    target: v_joined
`

describe('appendActionSource', () => {
  it('converts a single-string source into a two-element list', () => {
    const { handle, doc } = mustSelect(FAN_IN)
    appendActionSource(doc, 'join', 'v_b')
    const join = listActions(doc).find((a) => a.name === 'join')!
    expect(join.raw.source).toEqual(['v_a', 'v_b'])
    // Serialize round-trips as a real list.
    expect(serializeFlowgroupFile(handle)).toContain('- v_a')
  })

  it('builds the canonical fan-in: two inbound data edges appear on the join', () => {
    const { doc } = mustSelect(FAN_IN)
    appendActionSource(doc, 'join', 'v_b')
    const graph = deriveGraph(doc)
    const inbound = graph.edges.filter((e) => e.to === 'join' && e.kind === 'data')
    expect(inbound.map((e) => e.viewName).sort()).toEqual(['v_a', 'v_b'])
    expect(inbound.map((e) => e.from).sort()).toEqual(['load_a', 'load_b'])
  })

  it('appends to an existing list surgically and dedupes', () => {
    const { doc } = mustSelect(FAN_IN)
    appendActionSource(doc, 'join', 'v_b')
    appendActionSource(doc, 'join', 'v_c')
    appendActionSource(doc, 'join', 'v_c') // no-op (already present)
    const join = listActions(doc).find((a) => a.name === 'join')!
    expect(join.raw.source).toEqual(['v_a', 'v_b', 'v_c'])
  })

  it('sets a bare string when the source is absent', () => {
    const src = `pipeline: p
flowgroup: fg
actions:
  - name: w
    type: write
    write_target:
      type: streaming_table
`
    const { doc } = mustSelect(src)
    appendActionSource(doc, 'w', 'v_upstream')
    const w = listActions(doc).find((a) => a.name === 'w')!
    expect(w.raw.source).toBe('v_upstream')
  })

  it('refuses a structured (load) source', () => {
    const { doc } = mustSelect(FAN_IN)
    expect(() => appendActionSource(doc, 'load_a', 'v_other')).toThrow(/structured source/)
  })
})
