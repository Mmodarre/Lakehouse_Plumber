import { afterEach, describe, expect, it, vi } from 'vitest'
import { render, screen, fireEvent, cleanup } from '@testing-library/react'

import { GraphStaleBadge } from '../GraphStaleBadge'
import type { UseGraphStalenessResult } from '../../../hooks/useGraphStaleness'

const useGraphStaleness = vi.hoisted(() => vi.fn<() => UseGraphStalenessResult>())
vi.mock('../../../hooks/useGraphStaleness', () => ({ useGraphStaleness }))

afterEach(() => {
  cleanup()
  vi.clearAllMocks()
})

describe('GraphStaleBadge', () => {
  it('renders nothing while the graph is fresh', () => {
    useGraphStaleness.mockReturnValue({ isStale: false, refresh: vi.fn(), isRefreshing: false })
    const { container } = render(<GraphStaleBadge />)
    expect(container).toBeEmptyDOMElement()
  })

  it('shows the Refresh affordance and fires refresh on click when stale', () => {
    const refresh = vi.fn()
    useGraphStaleness.mockReturnValue({ isStale: true, refresh, isRefreshing: false })
    render(<GraphStaleBadge />)

    expect(screen.getByText('Graph out of date')).toBeInTheDocument()
    fireEvent.click(screen.getByRole('button', { name: /refresh/i }))
    expect(refresh).toHaveBeenCalledTimes(1)
  })

  it('disables the button and shows a spinner label while refreshing', () => {
    useGraphStaleness.mockReturnValue({ isStale: true, refresh: vi.fn(), isRefreshing: true })
    render(<GraphStaleBadge />)

    const button = screen.getByRole('button', { name: /refreshing/i })
    expect(button).toBeDisabled()
  })
})
