/**
 * Compose ops used by the designer's node context actions (Task 9):
 * duplicate + delete, with edge recomputation. The load-bearing guarantee is
 * that deleting a producer that others consume does NOT cascade — the
 * downstream source becomes a dangling external ref.
 */
import { describe, expect, it } from 'vitest'

import {
  deleteAction,
  deriveGraph,
  duplicateAction,
  listActions,
  parseFlowgroupFile,
  selectFlowgroup,
} from '../flowgroup-doc'
import type { FlowgroupDocHandle } from '../flowgroup-doc'

const SRC = `pipeline: p
flowgroup: fg
actions:
  - name: load_orders
    type: load
    source:
      type: delta
      table: orders
    target: v_orders
  - name: clean_orders
    type: transform
    transform_type: sql
    source: v_orders
    target: v_clean
`

function doc(): FlowgroupDocHandle {
  return selectFlowgroup(parseFlowgroupFile(SRC), 'fg')!
}

describe('duplicateAction', () => {
  it('inserts a uniquely-named copy right after the original', () => {
    const d = doc()
    const newName = duplicateAction(d, 'load_orders')
    expect(newName).toBe('load_orders_copy')
    const names = listActions(d).map((a) => a.name)
    expect(names).toEqual(['load_orders', 'load_orders_copy', 'clean_orders'])
  })
})

describe('deleteAction (no cascade)', () => {
  it('deleting a consumed producer leaves the consumer with a dangling external ref', () => {
    const d = doc()
    deleteAction(d, 'load_orders')

    // The consumer survives (no cascade).
    const names = listActions(d).map((a) => a.name)
    expect(names).toEqual(['clean_orders'])

    // Its source view no longer has an in-flowgroup producer → external node.
    const graph = deriveGraph(d)
    expect(graph.nodes.map((n) => n.id)).toEqual(['clean_orders'])
    expect(graph.externals.map((x) => x.label)).toContain('v_orders')
    const edge = graph.edges.find((e) => e.to === 'clean_orders' && e.viewName === 'v_orders')
    expect(edge?.from).toBe('ext:v_orders')
  })
})
