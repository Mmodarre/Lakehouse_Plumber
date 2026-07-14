// Concurrency regression tests for the ActionForm list editors.
//
// The real designer commits each edit through an ASYNC write (parse cache →
// mutate → PUT → re-derive). Two structural clicks fired inside one write
// window therefore share the SAME render — and so the SAME closures. This
// harness reproduces that window: `commit` queues the mutator (it does NOT
// re-render synchronously the way a naive harness would), and `flush` applies
// the queue against the chain exactly as useDesignerWrite does — each mutator
// re-parses the latest committed text. `gate` models a write being in flight
// (useDesignerWrite.saving), which disables the structural controls.
//
// Three confirmed-live repros, each FAILS against the pre-fix closures and
// PASSES after:
//   (a) double-click Add on an objectList must yield TWO rows (not lose one).
//   (b) a racing second Remove must not delete a shifted (wrong) row.
//   (c) clearing via racing Removes must not leave an orphan empty list.

import { useEffect, useState } from 'react'
import { describe, expect, it } from 'vitest'
import { act, fireEvent, render, screen } from '@testing-library/react'
import {
  listActions,
  parseFlowgroupFile,
  selectFlowgroup,
  serializeFlowgroupFile,
} from '@/lib/flowgroup-doc'
import type { DesignerMutator } from '../formModel'
import { ActionForm } from '../ActionForm'
import { flowgroupProviders } from './actionFormTestSupport'
import { readPath } from '../specs/helpers'
import { testCustomExpectationsSpec } from '../specs/test-custom-expectations'

const FG = 'fg_ce'
const ACTION = 'ta_rules'

const HEAD = `pipeline: perf
flowgroup: fg_ce
actions:
  - name: ta_rules
    type: test
    test_type: custom_expectations
    source: v_orders
`

const NO_ROWS = HEAD

const TWO_ROWS = `${HEAD}    expectations:
      - name: positive_amount
        expression: "total_price > 0"
      - name: reasonable_discount
        expression: "discount_percent <= 50"
`

const THREE_ROWS = `${HEAD}    expectations:
      - name: r1
        expression: "a > 0"
      - name: r2
        expression: "b > 0"
      - name: r3
        expression: "c > 0"
`

interface Handle {
  /** The action's `expectations` value as currently committed (undefined when the key is gone). */
  rows: () => unknown
  /** Apply every queued mutator against the chain, then re-render. */
  flush: () => void
}

/**
 * Render a real ActionForm whose `commit` DEFERS application to `flush`, so two
 * clicks land in one write window. `gate: true` flips `saving` on the first
 * commit (a write in flight), disabling the structural controls.
 */
function renderConcurrent(yaml: string, opts: { gate: boolean }): Handle {
  const committed = { text: yaml }
  const queue: DesignerMutator[] = []
  const ctl = {} as { setText: (s: string) => void; setSaving: (b: boolean) => void }

  function Harness() {
    const [text, setText] = useState(yaml)
    const [saving, setSaving] = useState(false)
    // Register the (stable) setters for `flush`; an effect keeps ESLint's
    // no-mutation-during-render rule happy.
    useEffect(() => {
      ctl.setText = setText
      ctl.setSaving = setSaving
    }, [])
    const fg = selectFlowgroup(parseFlowgroupFile(text), FG)!
    const action = listActions(fg).find((a) => a.name === ACTION)!
    return (
      <ActionForm
        spec={testCustomExpectationsSpec}
        action={action}
        actionId={ACTION}
        commit={(mutator) => {
          queue.push(mutator)
          if (opts.gate) setSaving(true)
        }}
        rename={async () => true}
        readOnly={false}
        saving={saving}
        presetBadges={[]}
        onRenamed={() => {}}
        onEditCode={() => {}}
      />
    )
  }

  render(<Harness />, { wrapper: flowgroupProviders() })

  return {
    rows: () => {
      const fg = selectFlowgroup(parseFlowgroupFile(committed.text), FG)!
      const action = listActions(fg).find((a) => a.name === ACTION)!
      return readPath(action.raw, ['expectations'])
    },
    flush: () =>
      act(() => {
        for (const mutator of queue) {
          const file = parseFlowgroupFile(committed.text)
          mutator(selectFlowgroup(file, FG)!)
          committed.text = serializeFlowgroupFile(file)
        }
        queue.length = 0
        ctl.setText(committed.text)
        ctl.setSaving(false)
      }),
  }
}

describe('ActionForm — concurrent list edits', () => {
  it('(a) double-click Add appends two rows (no lost update)', () => {
    const h = renderConcurrent(NO_ROWS, { gate: false })
    const addBtn = screen.getByRole('button', { name: 'Add Expectations entry' })
    // Two clicks inside one write window (Add is deliberately NOT gated).
    fireEvent.click(addBtn)
    fireEvent.click(addBtn)
    h.flush()
    // Pre-fix: both closures wrote at the stale length → one row survives.
    expect(h.rows()).toHaveLength(2)
  })

  it('(b) a racing second Remove cannot delete a shifted row', () => {
    const h = renderConcurrent(THREE_ROWS, { gate: true })
    const remove1 = screen.getByRole('button', { name: 'Remove Expectations entry 1' })
    fireEvent.click(remove1)
    // The in-flight write disables every Remove, so the racing click is dropped.
    expect(remove1).toBeDisabled()
    fireEvent.click(remove1)
    h.flush()
    const rows = h.rows() as unknown[]
    // Only entry 1 (r1) removed; pre-fix the racing click also deleted r2.
    expect(rows).toHaveLength(2)
    expect((rows[0] as { name: string }).name).toBe('r2')
    expect((rows[1] as { name: string }).name).toBe('r3')
  })

  it('(c) racing Removes clear the key instead of orphaning an empty list', () => {
    const h = renderConcurrent(TWO_ROWS, { gate: false })
    // Both land in one window; the delete-on-clear guard is read from the doc.
    fireEvent.click(screen.getByRole('button', { name: 'Remove Expectations entry 2' }))
    fireEvent.click(screen.getByRole('button', { name: 'Remove Expectations entry 1' }))
    h.flush()
    // Pre-fix: the stale length guard left `expectations: []`.
    expect(h.rows()).toBeUndefined()
  })
})
