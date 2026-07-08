import { describe, expect, it, vi } from 'vitest'
import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { EmptyState } from '@/components/common/EmptyState'

// Smoke test for a shadcn-composed component: EmptyState renders a lucide
// icon + text and composes ui/button for its optional action.

describe('EmptyState', () => {
  it('renders the default title and message', () => {
    render(<EmptyState />)
    expect(screen.getByText('No data')).toBeInTheDocument()
    expect(screen.getByText('Nothing to display yet.')).toBeInTheDocument()
  })

  it('renders a custom title/message without an action button', () => {
    render(<EmptyState title="No pipelines" message="Create one to get started." />)
    expect(screen.getByText('No pipelines')).toBeInTheDocument()
    expect(screen.getByText('Create one to get started.')).toBeInTheDocument()
    expect(screen.queryByRole('button')).not.toBeInTheDocument()
  })

  it('renders the action as a button and fires its handler on click', async () => {
    const user = userEvent.setup()
    const onClick = vi.fn()
    render(<EmptyState action={{ label: 'New flowgroup', onClick }} />)

    const button = screen.getByRole('button', { name: 'New flowgroup' })
    expect(button).toBeInTheDocument()
    await user.click(button)
    expect(onClick).toHaveBeenCalledTimes(1)
  })
})
