import { describe, expect, it, vi } from 'vitest'
import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { OptionalTextField } from '../OptionalTextField'

function setup(value: unknown = 'hello', issue?: string) {
  const onSet = vi.fn()
  const onUnset = vi.fn()
  render(
    <OptionalTextField
      id="field"
      label="Field"
      value={value}
      onSet={onSet}
      onUnset={onUnset}
      issue={issue}
    />,
  )
  return { onSet, onUnset, input: screen.getByLabelText('Field') }
}

describe('OptionalTextField', () => {
  it('commits on blur only — never per keystroke', async () => {
    const { onSet, input } = setup()
    const user = userEvent.setup()

    await user.clear(input)
    await user.type(input, 'world')
    expect(onSet).not.toHaveBeenCalled()

    await user.tab()
    expect(onSet).toHaveBeenCalledExactlyOnceWith('world')
  })

  it('commits on Enter', async () => {
    const { onSet, input } = setup()
    const user = userEvent.setup()
    await user.clear(input)
    await user.type(input, 'world{Enter}')
    expect(onSet).toHaveBeenCalledExactlyOnceWith('world')
  })

  it('an unchanged blur commits nothing', async () => {
    const { onSet, onUnset, input } = setup()
    const user = userEvent.setup()
    await user.click(input)
    await user.tab()
    expect(onSet).not.toHaveBeenCalled()
    expect(onUnset).not.toHaveBeenCalled()
  })

  it('an emptied field calls onUnset (delete the key), not onSet("")', async () => {
    const { onSet, onUnset, input } = setup()
    const user = userEvent.setup()
    await user.clear(input)
    await user.tab()
    expect(onUnset).toHaveBeenCalledOnce()
    expect(onSet).not.toHaveBeenCalled()
  })

  it('Escape reverts the draft without committing', async () => {
    const { onSet, onUnset, input } = setup()
    const user = userEvent.setup()
    await user.clear(input)
    await user.type(input, 'draft{Escape}')
    expect(input).toHaveValue('hello')
    await user.tab()
    expect(onSet).not.toHaveBeenCalled()
    expect(onUnset).not.toHaveBeenCalled()
  })

  it('displays a non-string value coerced and flags the issue passed in', () => {
    const { input } = setup(1.5, `'version' must be a string`)
    expect(input).toHaveValue('1.5')
    expect(input).toHaveAttribute('aria-invalid', 'true')
    expect(screen.getByRole('alert')).toHaveTextContent(`'version' must be a string`)
  })
})
