import { beforeEach, describe, expect, it, vi } from 'vitest'
import { act, render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { BottomPanel } from '../BottomPanel'
import { useLayoutStore } from '../../../../store/layoutStore'
import { useWorkspaceStore } from '../../../../store/workspaceStore'
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
  useWorkspaceStore.getState().closeAllBuffers()
  useLayoutStore.setState({ bottomCollapsed: false, bottomTab: 'problems', focusMode: false })
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
    expect(screen.getByText('Not validated')).toBeInTheDocument()
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
    expect(screen.getByText('Not validated')).toBeInTheDocument()
    expect(screen.getByTestId('problems-panel')).toBeInTheDocument()

    // Populate: the panel is still mounted, the empty-state visual is gone.
    act(() => {
      useRunStore.setState({ issues: [anIssue] })
    })
    expect(screen.getByTestId('problems-panel')).toBeInTheDocument()
    expect(screen.queryByText('Not validated')).not.toBeInTheDocument()

    // Clear: the panel is STILL mounted (never unmounted, so the
    // populated→empty change stays announceable) and the visual returns.
    act(() => {
      useRunStore.setState({ issues: [] })
    })
    expect(screen.getByTestId('problems-panel')).toBeInTheDocument()
    expect(screen.getByText('Not validated')).toBeInTheDocument()
  })

  it('distinguishes successful validation from unsaved changes and prior history', () => {
    useRunStore.setState({ runKind: 'validate', terminal: 'success' })
    const view = render(<BottomPanel />)
    expect(screen.getByText('No problems')).toBeInTheDocument()
    act(() => {
      useWorkspaceStore.getState().openBuffer('a.yaml', { content: 'saved', exists: true })
      useWorkspaceStore.getState().updateContent('a.yaml', 'edited')
    })
    expect(screen.getByText('Results need refresh')).toBeInTheDocument()
    act(() => {
      useWorkspaceStore.getState().closeAllBuffers()
      useRunStore.setState({ runKind: null, terminal: null, hydratedFrom: { runId: 'prior', startedAt: null, env: 'test', pipeline: null } })
    })
    view.rerender(<BottomPanel />)
    expect(screen.getByText('No recorded problems')).toBeInTheDocument()
  })

  it('hides expanded content from keyboard navigation in focus mode while retaining the pane', async () => {
    const user = userEvent.setup()
    useLayoutStore.setState({ focusMode: true, bottomCollapsed: false })
    render(<BottomPanel forceCollapsed />)
    expect(screen.getByTestId('problems-panel')).not.toBeVisible()
    expect(screen.queryByRole('separator', { name: 'Resize panel' })).not.toBeInTheDocument()
    expect(useLayoutStore.getState().bottomCollapsed).toBe(false)
    await user.click(screen.getByRole('tab', { name: /History/ }))
    expect(useLayoutStore.getState().focusMode).toBe(false)
  })

  it('switches to the Run and History panes', async () => {
    const user = userEvent.setup()
    render(<BottomPanel />)

    await user.click(screen.getByRole('tab', { name: /Run/ }))
    expect(await screen.findByTestId('run-stream')).toBeInTheDocument()
    expect(useLayoutStore.getState().bottomTab).toBe('run')

    await user.click(screen.getByRole('tab', { name: /History/ }))
    expect(await screen.findByTestId('run-history')).toBeInTheDocument()
  })

  it('retains panes first opened by a workspace command when switching away', async () => {
    render(<BottomPanel />)
    act(() => useLayoutStore.getState().setBottomTab('run'))
    const run = await screen.findByTestId('run-stream')
    act(() => useLayoutStore.getState().setBottomTab('problems'))
    expect(screen.getByTestId('run-stream')).toBe(run)
    expect(run).not.toBeVisible()
  })

  it('expands from collapsed when a tab is clicked, and the chevron toggles collapse', async () => {
    const user = userEvent.setup()
    useLayoutStore.setState({ bottomCollapsed: true, bottomTab: 'problems' })
    render(<BottomPanel />)

    // Collapsed: the body (empty state) is hidden but tabs stay visible.
    expect(screen.getByText('Not validated')).not.toBeVisible()

    await user.click(screen.getByRole('tab', { name: /Run/ }))
    expect(useLayoutStore.getState().bottomCollapsed).toBe(false)
    expect(useLayoutStore.getState().bottomTab).toBe('run')
    expect(await screen.findByTestId('run-stream')).toBeInTheDocument()

    await user.click(screen.getByRole('button', { name: /Collapse panel/ }))
    expect(useLayoutStore.getState().bottomCollapsed).toBe(true)
  })
})
