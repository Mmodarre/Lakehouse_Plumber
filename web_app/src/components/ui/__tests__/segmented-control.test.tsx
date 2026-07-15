import { useState } from 'react'
import { describe, expect, it, vi } from 'vitest'
import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'

import { SegmentedControl } from '../segmented-control'

type Option = { value: string; label: string; disabled?: boolean }

const OPTIONS: Option[] = [
  { value: 'a', label: 'Alpha' },
  { value: 'b', label: 'Bravo' },
  { value: 'c', label: 'Charlie', disabled: true },
]

// Stateful harness: mirrors the controlled contract so `data-state` updates in
// response to `onValueChange`, while the spy still observes forwarded values.
function Harness({
  initial = 'a',
  onChange,
  options = OPTIONS,
}: {
  initial?: string
  onChange?: (v: string) => void
  options?: Option[]
}) {
  const [value, setValue] = useState(initial)
  return (
    <SegmentedControl
      aria-label="Test group"
      value={value}
      onValueChange={(v) => {
        setValue(v)
        onChange?.(v)
      }}
      options={options}
    />
  )
}

describe('SegmentedControl', () => {
  it('marks the selected option with data-state="on" and the rest "off"', () => {
    render(<Harness initial="b" />)

    expect(screen.getByRole('radio', { name: 'Bravo' })).toHaveAttribute(
      'data-state',
      'on',
    )
    expect(screen.getByRole('radio', { name: 'Alpha' })).toHaveAttribute(
      'data-state',
      'off',
    )
  })

  it('exposes the aria-label on the group', () => {
    render(<Harness />)
    expect(screen.getByRole('radiogroup', { name: 'Test group' })).toBeInTheDocument()
  })

  it('calls onValueChange with the clicked value', async () => {
    const user = userEvent.setup()
    const onChange = vi.fn()
    render(<Harness onChange={onChange} />)

    await user.click(screen.getByRole('radio', { name: 'Bravo' }))

    expect(onChange).toHaveBeenCalledWith('b')
    expect(screen.getByRole('radio', { name: 'Bravo' })).toHaveAttribute(
      'data-state',
      'on',
    )
  })

  it('does not deselect when the active option is re-clicked (non-deselectable)', async () => {
    const user = userEvent.setup()
    const onChange = vi.fn()
    render(<Harness initial="a" onChange={onChange} />)

    await user.click(screen.getByRole('radio', { name: 'Alpha' }))

    // Radix would emit "" here; the guard must swallow it so the value never
    // goes empty.
    expect(onChange).not.toHaveBeenCalled()
    expect(screen.getByRole('radio', { name: 'Alpha' })).toHaveAttribute(
      'data-state',
      'on',
    )
  })

  it('moves selection via keyboard arrow navigation', async () => {
    const user = userEvent.setup()
    const onChange = vi.fn()
    render(<Harness initial="a" onChange={onChange} />)

    await user.tab()
    expect(screen.getByRole('radio', { name: 'Alpha' })).toHaveFocus()

    await user.keyboard('{ArrowRight}')
    await user.keyboard('{Enter}')

    expect(onChange).toHaveBeenCalledWith('b')
    expect(screen.getByRole('radio', { name: 'Bravo' })).toHaveAttribute(
      'data-state',
      'on',
    )
  })

  it('renders a disabled option that cannot be selected', async () => {
    const user = userEvent.setup()
    const onChange = vi.fn()
    render(<Harness initial="a" onChange={onChange} />)

    const disabled = screen.getByRole('radio', { name: 'Charlie' })
    expect(disabled).toBeDisabled()

    await user.click(disabled)

    expect(onChange).not.toHaveBeenCalled()
    expect(disabled).toHaveAttribute('data-state', 'off')
  })
})
