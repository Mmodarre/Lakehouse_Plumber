import { beforeEach, describe, expect, it, vi } from 'vitest'
import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { Inspector } from '../Inspector'
import { useLayoutStore } from '../../../../store/layoutStore'
import type { InspectorTab } from '../../../../store/layoutStore'
import { useRunStore } from '../../../../store/runStore'
import { useWorkspaceStore } from '../../../../store/workspaceStore'
import { useDocumentStore } from '../../../../store/documentStore'
import { useSelectionStore } from '../../../../store/selectionStore'
import { fetchFileContentWithMeta } from '../../../../api/files'
import type { ValidationIssue } from '../../../../types/api'

vi.mock('../../../../api/files', () => ({
  fetchFileContentWithMeta: vi.fn(),
}))

const fetchFileMock = vi.mocked(fetchFileContentWithMeta)

function makeIssue(overrides: Partial<ValidationIssue> = {}): ValidationIssue {
  return {
    code: 'LHP-VAL-001',
    category: 'validation',
    severity: 'error',
    title: 'Missing target table',
    details: null,
    pipeline_name: null,
    flowgroup_name: null,
    file_path: null,
    suggestions: [],
    context: {},
    doc_link: null,
    ...overrides,
  }
}

beforeEach(() => {
  vi.clearAllMocks()
  useRunStore.getState().reset()
  useWorkspaceStore.getState().closeAllBuffers()
  useWorkspaceStore.setState({ tabs: [], activePath: null })
  useDocumentStore.setState({ docs: {} })
  useSelectionStore.setState({ byTab: {} })
  useLayoutStore.setState({ inspectorCollapsed: false, inspectorTab: 'validation' })
})

describe('Inspector', () => {
  it('collapses to a rail whose Validation button expands + selects the tab', async () => {
    const user = userEvent.setup()
    useLayoutStore.setState({ inspectorCollapsed: true, inspectorTab: 'help' })
    render(<Inspector />)

    await user.click(screen.getByRole('button', { name: /Expand inspector — Validation/ }))
    expect(useLayoutStore.getState().inspectorCollapsed).toBe(false)
    expect(useLayoutStore.getState().inspectorTab).toBe('validation')
  })

  it('shows an empty state when there are no issues (project scope)', () => {
    render(<Inspector />)
    expect(screen.getByText('No issues')).toBeInTheDocument()
  })

  it('renders scoped issues and opens the offending file on row click', async () => {
    const user = userEvent.setup()
    useRunStore.setState({
      issues: [
        makeIssue({
          file_path: 'pipelines/bronze/customers.yaml',
          context: { line: 12 },
        }),
      ],
    })
    fetchFileMock.mockResolvedValue({ content: 'x: 1', etag: 'abc' })
    render(<Inspector />)

    expect(screen.getByText('customers.yaml:12')).toBeInTheDocument()
    await user.click(screen.getByRole('button', { name: /missing target table/i }))

    await waitFor(() =>
      expect(fetchFileMock).toHaveBeenCalledWith('pipelines/bronze/customers.yaml'),
    )
    await waitFor(() =>
      expect(
        useWorkspaceStore.getState().buffers.some(
          (b) => b.path === 'pipelines/bronze/customers.yaml',
        ),
      ).toBe(true),
    )
  })

  it('scopes issues to the active entity tab', () => {
    useRunStore.setState({
      issues: [
        makeIssue({ code: 'A', title: 'orders issue', pipeline_name: 'bronze', flowgroup_name: 'orders' }),
        makeIssue({ code: 'B', title: 'customers issue', pipeline_name: 'bronze', flowgroup_name: 'customers' }),
      ],
    })
    useWorkspaceStore
      .getState()
      .openEntityTab('bronze', 'orders', 'pipelines/bronze/orders.yaml', { view: 'graph' })
    render(<Inspector />)

    expect(screen.getByText('orders issue')).toBeInTheDocument()
    expect(screen.queryByText('customers issue')).not.toBeInTheDocument()
  })

  it('surfaces client-side config validator issues for the active config tab', () => {
    // A project config whose top level is a sequence trips the config-model
    // validator (`lhp.yaml must be a mapping`) — a client-side issue that never
    // enters runStore. Opening it as a config Form tab must surface it here.
    const ws = useWorkspaceStore.getState()
    ws.openBuffer('lhp.yaml', { content: '- a\n- b', exists: true, activate: false })
    useDocumentStore.getState().open('lhp.yaml', 'project')
    ws.openConfigTab('lhp.yaml', 'project')
    render(<Inspector />)

    expect(screen.getByText('lhp.yaml must be a mapping')).toBeInTheDocument()
  })

  it('shows the Help placeholder on the Help tab', async () => {
    const user = userEvent.setup()
    render(<Inspector />)
    await user.click(screen.getByRole('tab', { name: 'Help' }))
    expect(screen.getByText(/Field help arrives with the inspector routing task/)).toBeInTheDocument()
  })

  it('no longer renders an Action tab (editing moved to the graph modal — Fix #3)', () => {
    useLayoutStore.setState({ inspectorTab: 'validation' })
    render(<Inspector />)
    expect(screen.queryByRole('tab', { name: /^Action$/i })).not.toBeInTheDocument()
    expect(screen.getByRole('tab', { name: /validation/i })).toBeInTheDocument()
    expect(screen.getByRole('tab', { name: /help/i })).toBeInTheDocument()
  })

  it('safe-defaults an unexpected inspectorTab value to the Validation pane', () => {
    // The InspectorTab type no longer admits 'action' (field editing moved to
    // the graph modal — Fix #3; the layoutStore v2 migrate heals a stored
    // 'action' → 'validation'). Force a value the type forbids to prove the
    // component still renders Validation as its safe default.
    useLayoutStore.setState({ inspectorTab: 'action' as unknown as InspectorTab })
    render(<Inspector />)
    // No leftover Action empty-state; the Validation pane renders instead.
    expect(
      screen.queryByText('Select an action in the graph to edit it.'),
    ).not.toBeInTheDocument()
    expect(screen.getByText('No issues')).toBeInTheDocument()
  })

  it('the collapsed rail no longer offers an Action button', () => {
    useLayoutStore.setState({ inspectorCollapsed: true, inspectorTab: 'validation' })
    render(<Inspector />)
    expect(
      screen.queryByRole('button', { name: /Expand inspector — Action/ }),
    ).not.toBeInTheDocument()
    expect(
      screen.getByRole('button', { name: /Expand inspector — Validation/ }),
    ).toBeInTheDocument()
  })
})
