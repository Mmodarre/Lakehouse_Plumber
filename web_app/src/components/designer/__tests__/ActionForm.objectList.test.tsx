// Renderer-level tests for the objectList widget (Part 1 engine change): a
// real ActionForm is rendered against a live flowgroup handle whose `commit`
// applies the mutator and re-derives the action (the same write-through the
// designer uses, minus the network). Exercises add / edit / delete-row and
// delete-on-clear against the custom_expectations form.

import { useReducer } from 'react'
import { describe, expect, it } from 'vitest'
import { fireEvent, render, screen } from '@testing-library/react'
import {
  listActions,
  parseFlowgroupFile,
  selectFlowgroup,
  serializeFlowgroupFile,
} from '@/lib/flowgroup-doc'
import type { FlowgroupFileHandle } from '@/lib/flowgroup-doc'
import { ActionForm } from '../ActionForm'
import { flowgroupProviders } from './actionFormTestSupport'
import { readPath } from '../specs/helpers'
import { testCustomExpectationsSpec } from '../specs/test-custom-expectations'

const TWO_ROWS = `pipeline: perf
flowgroup: fg_ce
actions:
  - name: ta_rules
    type: test
    test_type: custom_expectations
    source: v_orders
    expectations:
      - name: positive_amount
        expression: "total_price > 0"
        on_violation: warn
      - name: reasonable_discount
        expression: "discount_percent <= 50"
        on_violation: warn
`

const ONE_ROW = `pipeline: perf
flowgroup: fg_ce
actions:
  - name: ta_rules
    type: test
    test_type: custom_expectations
    source: v_orders
    expectations:
      - name: only_rule
        expression: "x > 0"
`

function renderForm(yaml: string): { fileHandle: FlowgroupFileHandle } {
  const fileHandle = parseFlowgroupFile(yaml)
  const fg = selectFlowgroup(fileHandle, 'fg_ce')!
  function Harness() {
    const [, force] = useReducer((x: number) => x + 1, 0)
    const action = listActions(fg).find((a) => a.name === 'ta_rules')!
    return (
      <ActionForm
        spec={testCustomExpectationsSpec}
        action={action}
        actionId="ta_rules"
        commit={(mutator) => {
          mutator(fg)
          force()
        }}
        rename={async () => true}
        readOnly={false}
        presetBadges={[]}
        onRenamed={() => {}}
        onEditCode={() => {}}
      />
    )
  }
  render(<Harness />, { wrapper: flowgroupProviders() })
  return { fileHandle }
}

function currentRows(fileHandle: FlowgroupFileHandle): unknown {
  const reparsed = selectFlowgroup(parseFlowgroupFile(serializeFlowgroupFile(fileHandle)), 'fg_ce')!
  const action = listActions(reparsed).find((a) => a.name === 'ta_rules')!
  return readPath(action.raw, ['expectations'])
}

describe('ActionForm objectList — custom_expectations', () => {
  it('renders one card per expectation row', () => {
    renderForm(TWO_ROWS)
    expect(screen.getByDisplayValue('positive_amount')).toBeDefined()
    expect(screen.getByDisplayValue('discount_percent <= 50')).toBeDefined()
    expect(screen.getAllByLabelText('Name')).toHaveLength(2)
  })

  it('add-row appends an empty row and its item edits commit at the right path', () => {
    const { fileHandle } = renderForm(TWO_ROWS)
    fireEvent.click(screen.getByRole('button', { name: 'Add Expectations entry' }))
    expect(screen.getAllByLabelText('Name')).toHaveLength(3)

    const nameInputs = screen.getAllByLabelText('Name')
    fireEvent.change(nameInputs[2], { target: { value: 'third_rule' } })
    fireEvent.blur(nameInputs[2])

    const rows = currentRows(fileHandle) as Record<string, unknown>[]
    expect(rows).toHaveLength(3)
    expect(rows[2].name).toBe('third_rule')
  })

  it('editing an item field writes through without disturbing siblings', () => {
    const { fileHandle } = renderForm(TWO_ROWS)
    const exprInputs = screen.getAllByLabelText('Expression')
    fireEvent.change(exprInputs[0], { target: { value: 'total_price >= 1' } })
    fireEvent.blur(exprInputs[0])

    const rows = currentRows(fileHandle) as Record<string, unknown>[]
    expect(rows[0].expression).toBe('total_price >= 1')
    expect(rows[1].name).toBe('reasonable_discount')
  })

  it('remove-row deletes only that row', () => {
    const { fileHandle } = renderForm(TWO_ROWS)
    fireEvent.click(screen.getByRole('button', { name: 'Remove Expectations entry 1' }))

    const rows = currentRows(fileHandle) as Record<string, unknown>[]
    expect(rows).toHaveLength(1)
    expect(rows[0].name).toBe('reasonable_discount')
  })

  it('removing the last row deletes the whole key (delete-on-clear)', () => {
    const { fileHandle } = renderForm(ONE_ROW)
    fireEvent.click(screen.getByRole('button', { name: 'Remove Expectations entry 1' }))
    expect(currentRows(fileHandle)).toBeUndefined()
    // Empty state + add affordance remain.
    expect(screen.getByRole('button', { name: 'Add Expectations entry' })).toBeDefined()
  })
})
