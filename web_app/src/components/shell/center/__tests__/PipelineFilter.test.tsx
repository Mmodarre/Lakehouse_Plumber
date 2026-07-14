import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { render, screen, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'

import { PipelineFilter } from '../PipelineFilter'
import { useUIStore } from '../../../../store/uiStore'

// The scope value the mocked useSandboxScope returns (mutated per test).
const scopeRef = vi.hoisted(() => ({ current: null as Set<string> | null }))

vi.mock('../../../../hooks/useFlowgroups', () => ({
  useFlowgroups: () => ({
    data: {
      flowgroups: [
        { name: 'a1', pipeline: 'p_alpha', source_file: 'pipelines/p_alpha/a1.yaml', action_types: [] },
        { name: 'a2', pipeline: 'p_alpha', source_file: 'pipelines/p_alpha/a2.yaml', action_types: [] },
        { name: 'b1', pipeline: 'p_beta', source_file: 'pipelines/p_beta/b1.yaml', action_types: [] },
        { name: 'g1', pipeline: 'p_gamma', source_file: 'pipelines/p_gamma/g1.yaml', action_types: [] },
      ],
    },
  }),
}))

vi.mock('../../../sandbox/useSandboxScope', () => ({
  useSandboxScope: () => scopeRef.current,
}))

beforeEach(() => {
  scopeRef.current = null
  useUIStore.setState({ pipelineFilter: null })
  // Radix Popover + cmdk need DOM APIs jsdom lacks.
  Element.prototype.scrollIntoView = vi.fn()
  Element.prototype.hasPointerCapture = vi.fn(() => false) as never
  Element.prototype.setPointerCapture = vi.fn()
  Element.prototype.releasePointerCapture = vi.fn()
  vi.stubGlobal(
    'ResizeObserver',
    class {
      observe() {}
      unobserve() {}
      disconnect() {}
    },
  )
})

afterEach(() => {
  vi.unstubAllGlobals()
})

describe('ProjectMapView PipelineFilter — sandbox-scoped picker (item 6)', () => {
  it('lists every pipeline (deduped) when sandbox is off', async () => {
    const user = userEvent.setup()
    render(<PipelineFilter />)

    await user.click(screen.getByRole('combobox', { name: 'Filter by pipeline' }))
    const listbox = await screen.findByRole('listbox')

    expect(within(listbox).getByText('All pipelines')).toBeInTheDocument()
    expect(within(listbox).getByText('p_alpha')).toBeInTheDocument()
    expect(within(listbox).getByText('p_beta')).toBeInTheDocument()
    expect(within(listbox).getByText('p_gamma')).toBeInTheDocument()
  })

  it('shows only the sandbox-scoped subset when sandbox is on (not disabled)', async () => {
    scopeRef.current = new Set(['p_alpha'])
    const user = userEvent.setup()
    render(<PipelineFilter />)

    const trigger = screen.getByRole('combobox', { name: 'Filter by pipeline' })
    expect(trigger).not.toBeDisabled()

    await user.click(trigger)
    const listbox = await screen.findByRole('listbox')

    expect(within(listbox).getByText('p_alpha')).toBeInTheDocument()
    expect(within(listbox).queryByText('p_beta')).not.toBeInTheDocument()
    expect(within(listbox).queryByText('p_gamma')).not.toBeInTheDocument()
  })

  it('shows "All pipelines" when the stored filter is out of the sandbox scope (stale)', () => {
    // p_beta was picked with sandbox off, then a scope excluding it was enabled.
    scopeRef.current = new Set(['p_alpha'])
    useUIStore.setState({ pipelineFilter: 'p_beta' })
    render(<PipelineFilter />)

    const trigger = screen.getByRole('combobox', { name: 'Filter by pipeline' })
    expect(trigger).toHaveTextContent('All pipelines')
    expect(trigger).not.toHaveTextContent('p_beta')
  })

  it('selecting a pipeline drives uiStore.pipelineFilter', async () => {
    const user = userEvent.setup()
    render(<PipelineFilter />)

    await user.click(screen.getByRole('combobox', { name: 'Filter by pipeline' }))
    const listbox = await screen.findByRole('listbox')
    await user.click(within(listbox).getByText('p_beta'))

    expect(useUIStore.getState().pipelineFilter).toBe('p_beta')
  })
})
