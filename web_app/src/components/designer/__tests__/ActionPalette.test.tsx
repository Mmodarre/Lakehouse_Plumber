import { describe, expect, it, vi } from 'vitest'
import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'

import { ActionPalette } from '../ActionPalette'

describe('ActionPalette', () => {
  it('renders nothing while closed', () => {
    render(<ActionPalette open={false} title="Add action" onClose={vi.fn()} onPick={vi.fn()} />)
    expect(screen.queryByText('Add action')).not.toBeInTheDocument()
  })

  it('groups sub-types by kind and picks (kind, subType)', async () => {
    const user = userEvent.setup()
    const onPick = vi.fn()
    const onClose = vi.fn()
    render(<ActionPalette open title="Add action" onClose={onClose} onPick={onPick} />)

    // Kind group headings + a couple of representative sub-type titles.
    expect(screen.getByText('Load')).toBeInTheDocument()
    expect(screen.getByText('Transform')).toBeInTheDocument()
    expect(screen.getByText('Auto Loader (cloudFiles)')).toBeInTheDocument()

    await user.click(screen.getByText('SQL transform'))
    expect(onPick).toHaveBeenCalledWith('transform', 'sql')
    expect(onClose).toHaveBeenCalled()
  })

  it('filters sub-types by the search query', async () => {
    const user = userEvent.setup()
    render(<ActionPalette open title="Add action" onClose={vi.fn()} onPick={vi.fn()} />)

    await user.type(screen.getByLabelText(/filter action types/i), 'kafka')
    // The matching sub-type's mono discriminator badge is shown …
    expect(screen.getByText('kafka')).toBeInTheDocument()
    // … and a non-matching row is filtered out.
    expect(screen.queryByText('Auto Loader (cloudFiles)')).not.toBeInTheDocument()
  })

  it('shows the pre-wire subtitle when adding downstream', () => {
    render(
      <ActionPalette
        open
        title="Add downstream of load_orders"
        subtitle="New action reads v_orders"
        onClose={vi.fn()}
        onPick={vi.fn()}
      />,
    )
    expect(screen.getByText('Add downstream of load_orders')).toBeInTheDocument()
    expect(screen.getByText('New action reads v_orders')).toBeInTheDocument()
  })
})
