import { describe, expect, it, vi } from 'vitest'
import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { StringListEditor } from '../StringListEditor'

function setup(value: readonly unknown[] | undefined, allowEmpty = false) {
  const handlers = {
    onEditItem: vi.fn(),
    onAddItem: vi.fn(),
    onRemoveItem: vi.fn(),
    onDeleteKey: vi.fn(),
  }
  render(
    <StringListEditor
      id="list"
      label="Patterns"
      value={value}
      allowEmpty={allowEmpty}
      {...handlers}
    />,
  )
  return handlers
}

describe('StringListEditor', () => {
  it('absent key: shows "Not set"; empty list: shows "Empty list" (explicit distinction)', () => {
    setup(undefined)
    expect(screen.getByText('Not set')).toBeInTheDocument()
  })

  it('editing a row commits on blur through onEditItem', async () => {
    const { onEditItem } = setup(['a.yaml', 'b.yaml'])
    const user = userEvent.setup()
    const row = screen.getByLabelText('Patterns item 2')
    await user.clear(row)
    await user.type(row, 'c.yaml')
    expect(onEditItem).not.toHaveBeenCalled()
    await user.tab()
    expect(onEditItem).toHaveBeenCalledExactlyOnceWith(1, 'c.yaml')
  })

  it('adds via the add input on Enter and clears it', async () => {
    const { onAddItem } = setup(['a.yaml'])
    const user = userEvent.setup()
    const add = screen.getByLabelText('Patterns', { selector: 'input#list-add' })
    await user.type(add, 'new.yaml{Enter}')
    expect(onAddItem).toHaveBeenCalledExactlyOnceWith('new.yaml')
    expect(add).toHaveValue('')
  })

  it('removing a non-last row calls onRemoveItem', async () => {
    const { onRemoveItem, onDeleteKey } = setup(['a.yaml', 'b.yaml'])
    await userEvent.setup().click(screen.getByRole('button', { name: 'Remove Patterns item 1' }))
    expect(onRemoveItem).toHaveBeenCalledExactlyOnceWith(0)
    expect(onDeleteKey).not.toHaveBeenCalled()
  })

  it('removing the LAST row deletes the whole key (pristine absence)', async () => {
    const { onRemoveItem, onDeleteKey } = setup(['only.yaml'])
    await userEvent.setup().click(screen.getByRole('button', { name: 'Remove Patterns item 1' }))
    expect(onDeleteKey).toHaveBeenCalledOnce()
    expect(onRemoveItem).not.toHaveBeenCalled()
  })

  it('with allowEmpty, removing the last row keeps [] (onRemoveItem)', async () => {
    const { onRemoveItem, onDeleteKey } = setup(['only.yaml'], true)
    await userEvent.setup().click(screen.getByRole('button', { name: 'Remove Patterns item 1' }))
    expect(onRemoveItem).toHaveBeenCalledExactlyOnceWith(0)
    expect(onDeleteKey).not.toHaveBeenCalled()
    render(<StringListEditor id="l2" label="L2" value={[]} allowEmpty
      onEditItem={vi.fn()} onAddItem={vi.fn()} onRemoveItem={vi.fn()} onDeleteKey={vi.fn()} />)
    expect(screen.getByText('Empty list')).toBeInTheDocument()
  })
})
