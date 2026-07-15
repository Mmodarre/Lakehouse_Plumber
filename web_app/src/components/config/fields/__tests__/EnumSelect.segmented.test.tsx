import { beforeEach, describe, expect, it, vi } from 'vitest'
import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { EnumSelect } from '../EnumSelect'
import { coerceValue } from '@/components/designer/formModel'
import type { FieldSpec } from '@/components/designer/specs/types'

// The Radix Select (default, non-segmented path) needs the pointer-capture /
// scroll APIs jsdom lacks. The segmented path (Radix ToggleGroup) does not,
// but installing the stubs unconditionally keeps both paths safe.
beforeEach(() => {
  Element.prototype.scrollIntoView = vi.fn()
  Element.prototype.hasPointerCapture = vi.fn(() => false) as never
  Element.prototype.setPointerCapture = vi.fn()
  Element.prototype.releasePointerCapture = vi.fn()
})

describe('EnumSelect — segmented display', () => {
  it('display="segmented" renders a toggle-group (radiogroup), not a combobox', () => {
    render(
      <EnumSelect
        id="mode"
        label="Write mode"
        display="segmented"
        value="append"
        options={['append', 'overwrite', 'merge']}
        onSet={vi.fn()}
      />,
    )
    expect(screen.getByRole('radiogroup', { name: 'Write mode' })).toBeInTheDocument()
    expect(screen.queryByRole('combobox')).toBeNull()
    // Three concrete enum values only — no synthetic "none"/unset segment.
    expect(screen.getAllByRole('radio')).toHaveLength(3)
  })

  it('selecting a segment calls onSet with the chosen option string', async () => {
    const onSet = vi.fn()
    render(
      <EnumSelect
        id="mode"
        label="Write mode"
        display="segmented"
        value="append"
        options={['append', 'overwrite', 'merge']}
        onSet={onSet}
      />,
    )
    const user = userEvent.setup()
    await user.click(screen.getByRole('radio', { name: 'overwrite' }))
    expect(onSet).toHaveBeenCalledExactlyOnceWith('overwrite')
  })

  it('routes the chosen string through the caller coercion → integer enum yields a number', async () => {
    // EnumSelect always emits the raw option STRING; the caller's `onSet`
    // closure applies `valueType` coercion (exactly as ActionForm wires
    // `coerceValue`). The segmented branch must feed that SAME onSet path so an
    // integer enum still commits a number, not the label string.
    const received = vi.fn()
    const field = { path: [], label: 'SCD type', widget: 'enum', valueType: 'integer' } as FieldSpec
    render(
      <EnumSelect
        id="scd"
        label="SCD type"
        display="segmented"
        value="1"
        options={['1', '2']}
        onSet={(next) => received(coerceValue(field, next))}
      />,
    )
    const user = userEvent.setup()
    await user.click(screen.getByRole('radio', { name: '2' }))
    expect(received).toHaveBeenCalledExactlyOnceWith(2)
    expect(received.mock.calls[0][0]).toBeTypeOf('number')
  })

  it('display omitted → still renders the existing Select (combobox)', () => {
    render(
      <EnumSelect
        id="mode"
        label="Write mode"
        value="append"
        options={['append', 'overwrite', 'merge']}
        onSet={vi.fn()}
      />,
    )
    expect(screen.getByRole('combobox', { name: 'Write mode' })).toBeInTheDocument()
    expect(screen.queryByRole('radiogroup')).toBeNull()
  })
})
