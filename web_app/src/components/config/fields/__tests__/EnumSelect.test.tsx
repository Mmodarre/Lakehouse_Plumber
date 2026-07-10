import { beforeEach, describe, expect, it, vi } from 'vitest'
import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { EnumSelect } from '../EnumSelect'

// Radix Select needs the pointer-capture/scroll APIs jsdom lacks.
beforeEach(() => {
  Element.prototype.scrollIntoView = vi.fn()
  Element.prototype.hasPointerCapture = vi.fn(() => false) as never
  Element.prototype.setPointerCapture = vi.fn()
  Element.prototype.releasePointerCapture = vi.fn()
})

function setup(value: string | undefined) {
  const onSet = vi.fn()
  const onUnset = vi.fn()
  render(
    <EnumSelect
      id="strategy"
      label="Strategy"
      value={value}
      options={['table']}
      unsetLabel="Not set (default: table)"
      onSet={onSet}
      onUnset={onUnset}
    />,
  )
  return { onSet, onUnset }
}

describe('EnumSelect', () => {
  it('absent key shows the not-set entry; choosing an option SETS the key', async () => {
    const { onSet } = setup(undefined)
    const user = userEvent.setup()
    const trigger = screen.getByRole('combobox', { name: 'Strategy' })
    expect(trigger).toHaveTextContent('Not set (default: table)')

    await user.click(trigger)
    await user.click(await screen.findByRole('option', { name: 'table' }))
    expect(onSet).toHaveBeenCalledExactlyOnceWith('table')
  })

  it('choosing the not-set entry DELETES the key', async () => {
    const { onSet, onUnset } = setup('table')
    const user = userEvent.setup()
    await user.click(screen.getByRole('combobox', { name: 'Strategy' }))
    await user.click(await screen.findByRole('option', { name: 'Not set (default: table)' }))
    expect(onUnset).toHaveBeenCalledOnce()
    expect(onSet).not.toHaveBeenCalled()
  })
})
