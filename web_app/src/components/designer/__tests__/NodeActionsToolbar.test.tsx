import { describe, expect, it, vi } from 'vitest'
import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'

import { NodeActionsToolbar } from '../NodeActionsToolbar'

function renderToolbar(overrides: Partial<Parameters<typeof NodeActionsToolbar>[0]> = {}) {
  const props = {
    readOnly: false,
    producerViews: ['v_a', 'v_b'],
    canAddInput: true,
    onDuplicate: vi.fn(),
    onDelete: vi.fn(),
    onAddInput: vi.fn(),
    ...overrides,
  }
  render(<NodeActionsToolbar {...props} />)
  return props
}

describe('NodeActionsToolbar', () => {
  it('fires duplicate and delete', async () => {
    const user = userEvent.setup()
    const props = renderToolbar()
    await user.click(screen.getByRole('button', { name: /duplicate action/i }))
    await user.click(screen.getByRole('button', { name: /delete action/i }))
    expect(props.onDuplicate).toHaveBeenCalledOnce()
    expect(props.onDelete).toHaveBeenCalledOnce()
  })

  it('disables every op while read-only', () => {
    renderToolbar({ readOnly: true })
    expect(screen.getByRole('button', { name: /duplicate action/i })).toBeDisabled()
    expect(screen.getByRole('button', { name: /delete action/i })).toBeDisabled()
  })

  it('shows the add-input control only when fan-in applies and producers exist', () => {
    const { rerender } = render(
      <NodeActionsToolbar
        readOnly={false}
        producerViews={['v_a']}
        canAddInput
        onDuplicate={vi.fn()}
        onDelete={vi.fn()}
        onAddInput={vi.fn()}
      />,
    )
    expect(screen.getByLabelText(/add input view/i)).toBeInTheDocument()

    // No fan-in shape → no add-input control.
    rerender(
      <NodeActionsToolbar
        readOnly={false}
        producerViews={['v_a']}
        canAddInput={false}
        onDuplicate={vi.fn()}
        onDelete={vi.fn()}
        onAddInput={vi.fn()}
      />,
    )
    expect(screen.queryByLabelText(/add input view/i)).not.toBeInTheDocument()

    // Fan-in shape but no available producers → nothing to add.
    rerender(
      <NodeActionsToolbar
        readOnly={false}
        producerViews={[]}
        canAddInput
        onDuplicate={vi.fn()}
        onDelete={vi.fn()}
        onAddInput={vi.fn()}
      />,
    )
    expect(screen.queryByLabelText(/add input view/i)).not.toBeInTheDocument()
  })
})
