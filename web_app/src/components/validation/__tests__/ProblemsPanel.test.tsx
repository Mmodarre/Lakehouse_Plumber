import { describe, expect, it, vi, beforeEach } from 'vitest'
import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { ProblemsPanel } from '../ProblemsPanel'
import { useRunStore } from '../../../store/runStore'
import { useWorkspaceStore } from '../../../store/workspaceStore'
import { fetchFileContentWithMeta } from '../../../api/files'
import type { ValidationIssue } from '../../../types/api'

vi.mock('../../../api/files', () => ({
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
})

describe('ProblemsPanel', () => {
  it('keeps the sr-only status region mounted (empty) with no issues', () => {
    render(<ProblemsPanel />)
    expect(screen.getByRole('status')).toBeEmptyDOMElement()
    expect(screen.queryByText('Problems')).not.toBeInTheDocument()
  })

  it('announces problem counts in the always-mounted status region', () => {
    useRunStore.setState({
      issues: [
        makeIssue(),
        makeIssue({ code: 'LHP-VAL-002', title: 'Bad reference' }),
        makeIssue({ severity: 'warning', code: 'DEP-002', title: 'Unresolvable' }),
      ],
    })
    render(<ProblemsPanel />)
    expect(screen.getByRole('status')).toHaveTextContent(
      'Problems: 2 errors, 1 warning',
    )
  })

  it('renders issues through IssueList and opens the file as a workspace buffer', async () => {
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
    render(<ProblemsPanel />)

    // Shared IssueList row shape: code + message + file:line location.
    expect(screen.getByText('customers.yaml:12')).toBeInTheDocument()
    await user.click(screen.getByRole('button', { name: /missing target table/i }))

    await waitFor(() =>
      expect(fetchFileMock).toHaveBeenCalledWith('pipelines/bronze/customers.yaml'),
    )
    await waitFor(() =>
      expect(
        useWorkspaceStore
          .getState()
          .buffers.some((b) => b.path === 'pipelines/bronze/customers.yaml'),
      ).toBe(true),
    )
  })

  it('leaves rows without a file inert', async () => {
    const user = userEvent.setup()
    useRunStore.setState({ issues: [makeIssue({ file_path: null })] })
    render(<ProblemsPanel />)

    await user.click(screen.getByRole('button', { name: /missing target table/i }))
    expect(fetchFileMock).not.toHaveBeenCalled()
    expect(useWorkspaceStore.getState().buffers).toHaveLength(0)
  })
})
