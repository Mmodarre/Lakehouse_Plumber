import { describe, expect, it, vi } from 'vitest'
import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { KeyValueMapEditor } from '../KeyValueMapEditor'

function setup(value: Record<string, unknown> | undefined) {
  const handlers = {
    onSetEntry: vi.fn(),
    onRenameEntry: vi.fn(),
    onRemoveEntry: vi.fn(),
    onDeleteKey: vi.fn(),
  }
  render(<KeyValueMapEditor id="map" label="Defaults" value={value} {...handlers} />)
  return handlers
}

describe('KeyValueMapEditor', () => {
  it('string rows are editable and commit on blur', async () => {
    const { onSetEntry } = setup({ strategy: 'append' })
    const user = userEvent.setup()
    const valueInput = screen.getByLabelText('strategy value')
    await user.clear(valueInput)
    await user.type(valueInput, 'merge')
    await user.tab()
    expect(onSetEntry).toHaveBeenCalledExactlyOnceWith('strategy', 'merge')
  })

  it('non-string rows render read-only with a warning badge', () => {
    setup({ retention: 30 })
    expect(screen.getByLabelText('retention value')).toHaveAttribute('readonly')
    expect(screen.getByLabelText('retention key')).toHaveAttribute('readonly')
    expect(screen.getByText('not text')).toBeInTheDocument()
  })

  it('unlocking a non-string row allows the edit that coerces it', async () => {
    const { onSetEntry } = setup({ retention: 30 })
    const user = userEvent.setup()
    await user.click(screen.getByRole('button', { name: 'Edit retention as text' }))
    const valueInput = screen.getByLabelText('retention value')
    expect(valueInput).not.toHaveAttribute('readonly')
    await user.clear(valueInput)
    await user.type(valueInput, '45')
    await user.tab()
    expect(onSetEntry).toHaveBeenCalledExactlyOnceWith('retention', '45')
  })

  it('key rename goes through onRenameEntry; renaming onto an existing key is refused', async () => {
    const { onRenameEntry } = setup({ a: 'x', b: 'y' })
    const user = userEvent.setup()
    const keyInput = screen.getByLabelText('a key')

    await user.clear(keyInput)
    await user.type(keyInput, 'b')
    await user.tab()
    expect(onRenameEntry).not.toHaveBeenCalled()
    expect(screen.getByRole('alert')).toHaveTextContent(`Key 'b' already exists`)

    await user.clear(keyInput)
    await user.type(keyInput, 'c')
    await user.tab()
    expect(onRenameEntry).toHaveBeenCalledExactlyOnceWith('a', 'c')
  })

  it('removing the last entry deletes the whole key', async () => {
    const { onRemoveEntry, onDeleteKey } = setup({ only: 'x' })
    await userEvent.setup().click(screen.getByRole('button', { name: 'Remove only' }))
    expect(onDeleteKey).toHaveBeenCalledOnce()
    expect(onRemoveEntry).not.toHaveBeenCalled()
  })

  it('adds a new entry via the add row', async () => {
    const { onSetEntry } = setup({ a: 'x' })
    const user = userEvent.setup()
    await user.type(screen.getByLabelText('New Defaults key'), 'fresh')
    await user.type(screen.getByLabelText('New Defaults value'), 'v{Enter}')
    expect(onSetEntry).toHaveBeenCalledExactlyOnceWith('fresh', 'v')
  })
})
