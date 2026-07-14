import { beforeEach, describe, expect, it, vi } from 'vitest'
import { act, render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { BottomPanel } from '../BottomPanel'
import { useLayoutStore } from '../../../../store/layoutStore'
import { useRunStore } from '../../../../store/runStore'

// The panes are recompositions of already-tested surfaces; here they stand in
// as sentinels so the BottomPanel test focuses on tab / collapse behaviour.
vi.mock('../RunStreamView', () => ({ RunStreamView: () => <div data-testid="run-stream" /> }))
vi.mock('../RunHistoryView', () => ({ RunHistoryView: () => <div data-testid="run-history" /> }))
vi.mock('../../../validation/ProblemsPanel', () => ({
  ProblemsPanel: () => <div data-testid="problems-panel" />,
}))

beforeEach(() => {
  useRunStore.getState().reset()
  useLayoutStore.setState({ bottomCollapsed: false, bottomTab: 'problems' })
})

describe('BottomPanel', () => {
  it('always shows the three tab buttons', () => {
    render(<BottomPanel />)
    expect(screen.getByRole('tab', { name: /Problems/ })).toBeInTheDocument()
    expect(screen.getByRole('tab', { name: /Run/ })).toBeInTheDocument()
    expect(screen.getByRole('tab', { name: /History/ })).toBeInTheDocument()
  })

  it('renders the Problems empty state with no issues', () => {
    render(<BottomPanel />)
    expect(screen.getByText('No problems')).toBeInTheDocument()
  })

  const anIssue = {
    code: 'X',
    category: 'validation',
    severity: 'error' as const,
    title: 't',
    details: null,
    pipeline_name: null,
    flowgroup_name: null,
    file_path: null,
    suggestions: [],
    context: {},
    doc_link: null,
  }

  it('renders ProblemsPanel once issues exist', () => {
    useRunStore.setState({ issues: [anIssue] })
    render(<BottomPanel />)
    expect(screen.getByTestId('problems-panel')).toBeInTheDocument()
  })

  it('keeps ProblemsPanel mounted across empty↔populated transitions (persistent live region)', () => {
    render(<BottomPanel />)
    // Empty: the zero-state visual shows, but ProblemsPanel (which owns the
    // persistent sr-only role="status" region) stays mounted rather than being
    // swapped out for the EmptyState.
    expect(screen.getByText('No problems')).toBeInTheDocument()
    expect(screen.getByTestId('problems-panel')).toBeInTheDocument()

    // Populate: the panel is still mounted, the empty-state visual is gone.
    act(() => {
      useRunStore.setState({ issues: [anIssue] })
    })
    expect(screen.getByTestId('problems-panel')).toBeInTheDocument()
    expect(screen.queryByText('No problems')).not.toBeInTheDocument()

    // Clear: the panel is STILL mounted (never unmounted, so the
    // populated→empty change stays announceable) and the visual returns.
    act(() => {
      useRunStore.setState({ issues: [] })
    })
    expect(screen.getByTestId('problems-panel')).toBeInTheDocument()
    expect(screen.getByText('No problems')).toBeInTheDocument()
  })

  it('switches to the Run and History panes', async () => {
    const user = userEvent.setup()
    render(<BottomPanel />)

    await user.click(screen.getByRole('tab', { name: /Run/ }))
    expect(screen.getByTestId('run-stream')).toBeInTheDocument()
    expect(useLayoutStore.getState().bottomTab).toBe('run')

    await user.click(screen.getByRole('tab', { name: /History/ }))
    expect(screen.getByTestId('run-history')).toBeInTheDocument()
  })

  it('expands from collapsed when a tab is clicked, and the chevron toggles collapse', async () => {
    const user = userEvent.setup()
    useLayoutStore.setState({ bottomCollapsed: true, bottomTab: 'problems' })
    render(<BottomPanel />)

    // Collapsed: the body (empty state) is hidden but tabs stay visible.
    expect(screen.queryByText('No problems')).not.toBeInTheDocument()

    await user.click(screen.getByRole('tab', { name: /Run/ }))
    expect(useLayoutStore.getState().bottomCollapsed).toBe(false)
    expect(useLayoutStore.getState().bottomTab).toBe('run')
    expect(screen.getByTestId('run-stream')).toBeInTheDocument()

    await user.click(screen.getByRole('button', { name: /Collapse panel/ }))
    expect(useLayoutStore.getState().bottomCollapsed).toBe(true)
  })
})
