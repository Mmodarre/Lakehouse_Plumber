import { describe, expect, it, vi } from 'vitest'
import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { BoolSwitch } from '../BoolSwitch'

function setup(value: boolean | undefined, defaultValue = true) {
  const onSet = vi.fn()
  const onReset = vi.fn()
  render(
    <BoolSwitch
      id="flag"
      label="Flag"
      value={value}
      defaultValue={defaultValue}
      onSet={onSet}
      onReset={onReset}
    />,
  )
  return { onSet, onReset, toggle: screen.getByRole('switch', { name: 'Flag' }) }
}

describe('BoolSwitch (tri-state)', () => {
  it('unset: shows the inherited default subtly and no reset affordance', () => {
    const { toggle } = setup(undefined, true)
    expect(toggle).toBeChecked()
    expect(screen.getByText('default: on')).toBeInTheDocument()
    expect(screen.queryByRole('button', { name: 'Reset to default' })).not.toBeInTheDocument()
  })

  it('toggling from unset SETS the key explicitly (opposite of the default)', async () => {
    const { onSet, toggle } = setup(undefined, true)
    await userEvent.setup().click(toggle)
    expect(onSet).toHaveBeenCalledExactlyOnceWith(false)
  })

  it('explicit value: shows it and toggling sets the opposite', async () => {
    const { onSet, toggle } = setup(false, true)
    expect(toggle).not.toBeChecked()
    expect(screen.queryByText(/default:/)).not.toBeInTheDocument()
    await userEvent.setup().click(toggle)
    expect(onSet).toHaveBeenCalledExactlyOnceWith(true)
  })

  it('explicit value: "Reset to default" DELETES the key', async () => {
    const { onSet, onReset } = setup(true)
    await userEvent.setup().click(screen.getByRole('button', { name: 'Reset to default' }))
    expect(onReset).toHaveBeenCalledOnce()
    expect(onSet).not.toHaveBeenCalled()
  })
})
