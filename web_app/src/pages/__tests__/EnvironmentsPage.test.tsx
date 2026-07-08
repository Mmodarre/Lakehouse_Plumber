import { describe, expect, it, vi, beforeEach } from 'vitest'
import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { toast } from 'sonner'
import { EnvironmentsPage } from '../EnvironmentsPage'
import { useEnvironments, useEnvironmentResolved } from '../../hooks/useEnvironments'
import { useWorkspaceStore } from '../../store/workspaceStore'
import { fetchFileContentWithMeta } from '../../api/files'

vi.mock('../../hooks/useEnvironments', () => ({
  useEnvironments: vi.fn(),
  useEnvironmentResolved: vi.fn(),
}))
vi.mock('../../api/files', () => ({
  fetchFileContentWithMeta: vi.fn(),
}))
vi.mock('sonner', () => ({
  toast: { success: vi.fn(), error: vi.fn() },
}))

const environmentsMock = vi.mocked(useEnvironments)
const resolvedMock = vi.mocked(useEnvironmentResolved)
const fetchFileMock = vi.mocked(fetchFileContentWithMeta)

beforeEach(() => {
  vi.clearAllMocks()
  useWorkspaceStore.getState().closeAllBuffers()
  environmentsMock.mockReturnValue({
    data: { environments: ['dev', 'prod'], total: 2 },
    isLoading: false,
    isError: false,
    error: null,
  } as unknown as ReturnType<typeof useEnvironments>)
  resolvedMock.mockReturnValue({
    data: {
      env: 'dev',
      tokens: { catalog: 'main' },
      default_secret_scope: null,
      secret_references: [],
      raw_mappings: {},
    },
    isLoading: false,
    isError: false,
    error: null,
  } as unknown as ReturnType<typeof useEnvironmentResolved>)
})

describe('EnvironmentsPage edit opener', () => {
  it('opens substitutions/<env>.yaml as a workspace buffer', async () => {
    const user = userEvent.setup()
    fetchFileMock.mockResolvedValue({ content: 'dev:\n  catalog: main\n', etag: 'e1' })
    render(<EnvironmentsPage />)

    await user.click(screen.getByRole('button', { name: /edit substitutions/i }))

    await waitFor(() =>
      expect(fetchFileMock).toHaveBeenCalledWith('substitutions/dev.yaml'),
    )
    await waitFor(() =>
      expect(
        useWorkspaceStore
          .getState()
          .buffers.some((b) => b.path === 'substitutions/dev.yaml'),
      ).toBe(true),
    )
  })

  it('targets the selected environment tab', async () => {
    const user = userEvent.setup()
    fetchFileMock.mockResolvedValue({ content: '', etag: null })
    render(<EnvironmentsPage />)

    await user.click(screen.getByRole('tab', { name: 'prod' }))
    await user.click(screen.getByRole('button', { name: /edit substitutions/i }))

    await waitFor(() =>
      expect(fetchFileMock).toHaveBeenCalledWith('substitutions/prod.yaml'),
    )
  })

  it('surfaces a toast when the substitutions file cannot be fetched', async () => {
    const user = userEvent.setup()
    fetchFileMock.mockRejectedValue(new Error('boom'))
    render(<EnvironmentsPage />)

    await user.click(screen.getByRole('button', { name: /edit substitutions/i }))

    await waitFor(() => expect(toast.error).toHaveBeenCalled())
    expect(useWorkspaceStore.getState().buffers).toHaveLength(0)
  })
})
