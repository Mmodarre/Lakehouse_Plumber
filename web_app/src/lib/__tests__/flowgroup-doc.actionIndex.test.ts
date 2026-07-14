/**
 * resolveActionIndex must recognise the synthetic `__action_<index>` id that
 * deriveGraph mints for an unnamed action, so editing that action from the
 * canvas writes the right slot instead of silently no-opping. resolveActionIndex
 * is module-private, so it is exercised through the public setActionField.
 */
import { describe, expect, it } from 'vitest'

import {
  deriveGraph,
  listActions,
  parseFlowgroupFile,
  selectFlowgroup,
  setActionField,
} from '../flowgroup-doc'
import type { FlowgroupDocHandle } from '../flowgroup-doc'

// First action is intentionally unnamed (no `name` key); second is named.
const SRC = `pipeline: p
flowgroup: fg
actions:
  - type: load
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

describe('resolveActionIndex — synthetic __action_<index> id', () => {
  it('deriveGraph mints __action_0 for the unnamed first action', () => {
    const d = doc()
    expect(listActions(d)[0].name).toBe('')
    const node = deriveGraph(d).nodes[0]
    expect(node.id).toBe('__action_0')
    expect(node.actionIndex).toBe(0)
  })

  it('setActionField on __action_0 resolves to list index 0 and writes it (round-trip)', () => {
    const d = doc()
    setActionField(d, '__action_0', ['target'], 'v_written')

    const actions = listActions(d)
    // The synthetic id resolved to the first list slot...
    expect(actions[0].target).toBe('v_written')
    // ...and the named second action was left untouched.
    expect(actions[1].name).toBe('clean_orders')
    expect(actions[1].target).toBe('v_clean')
  })
})
