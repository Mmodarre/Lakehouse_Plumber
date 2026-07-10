import { describe, expect, it, vi } from 'vitest'
import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { OptionalNumberField } from '../OptionalNumberField'

function setup(value: unknown = 10) {
  const onSet = vi.fn()
  const onUnset = vi.fn()
  render(
    <OptionalNumberField
      id="num"
      label="Num"
      value={value}
      min={1}
      max={20}
      onSet={onSet}
      onUnset={onUnset}
    />,
  )
  return { onSet, onUnset, input: screen.getByLabelText('Num') }
}

describe('OptionalNumberField', () => {
  it('commits a valid integer on blur', async () => {
    const { onSet, input } = setup()
    const user = userEvent.setup()
    await user.clear(input)
    await user.type(input, '15')
    expect(onSet).not.toHaveBeenCalled() // not per keystroke
    await user.tab()
    expect(onSet).toHaveBeenCalledExactlyOnceWith(15)
  })

  it('empty commit deletes the key', async () => {
    const { onSet, onUnset, input } = setup()
    const user = userEvent.setup()
    await user.clear(input)
    await user.tab()
    expect(onUnset).toHaveBeenCalledOnce()
    expect(onSet).not.toHaveBeenCalled()
  })

  it('refuses a non-integer: local error, nothing committed', async () => {
    const { onSet, onUnset, input } = setup()
    const user = userEvent.setup()
    await user.clear(input)
    await user.type(input, '1.5{Enter}')
    expect(screen.getByRole('alert')).toHaveTextContent('Must be a whole number')
    expect(onSet).not.toHaveBeenCalled()
    expect(onUnset).not.toHaveBeenCalled()
  })

  it('refuses an out-of-range integer: local error names the bounds', async () => {
    const { onSet, input } = setup()
    const user = userEvent.setup()
    await user.clear(input)
    await user.type(input, '99{Enter}')
    expect(screen.getByRole('alert')).toHaveTextContent('Must be between 1 and 20')
    expect(onSet).not.toHaveBeenCalled()
  })
})
