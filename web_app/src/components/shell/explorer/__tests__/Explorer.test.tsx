import { beforeEach, describe, expect, it, vi } from 'vitest'
import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { Explorer } from '../Explorer'
import { useLayoutStore } from '../../../../store/layoutStore'

// Isolate the host from the lens bodies (each lens has its own tests).
vi.mock('../StructureLens', () => ({ StructureLens: () => <div data-testid="lens-structure" /> }))
vi.mock('../TablesLens', () => ({ TablesLens: () => <div data-testid="lens-tables" /> }))
vi.mock('../FilesLens', () => ({ FilesLens: () => <div data-testid="lens-files" /> }))

beforeEach(() => {
  useLayoutStore.setState({ explorerLens: 'structure', explorerCollapsed: false })
})

describe('Explorer', () => {
  it('renders the active lens and a resize handle', () => {
    render(<Explorer />)
    expect(screen.getByTestId('lens-structure')).toBeInTheDocument()
    expect(screen.getByRole('separator', { name: 'Resize explorer' })).toBeInTheDocument()
  })

  it('switches lens via the switcher and writes layoutStore.explorerLens', async () => {
    render(<Explorer />)
    await userEvent.click(screen.getByRole('tab', { name: 'Tables' }))
    expect(useLayoutStore.getState().explorerLens).toBe('tables')
    expect(screen.getByTestId('lens-tables')).toBeInTheDocument()
    expect(screen.queryByTestId('lens-structure')).not.toBeInTheDocument()
  })

  it('hosts Structure directly in a flex column (it owns its scroll) but wraps Tables in a scroll region', async () => {
    render(<Explorer />)
    // Structure lens fills the flex column itself — no outer overflow-auto, so
    // its pinned Config/Resources region can stay put while the tree scrolls.
    const structureParent = screen.getByTestId('lens-structure').parentElement
    expect(structureParent?.className).toContain('flex-col')
    expect(structureParent?.className).not.toContain('overflow-auto')
    // Tables/Files still get their own scroll container.
    await userEvent.click(screen.getByRole('tab', { name: 'Tables' }))
    expect(screen.getByTestId('lens-tables').parentElement?.className).toContain('overflow-auto')
  })

  it('renders nothing when collapsed', () => {
    useLayoutStore.setState({ explorerCollapsed: true })
    render(<Explorer />)
    expect(screen.queryByRole('navigation', { name: 'Entity explorer' })).not.toBeInTheDocument()
  })
})
