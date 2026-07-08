import { describe, expect, it, vi, beforeEach } from 'vitest'
import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { InitProjectPage } from '../InitProjectPage'
import { initProject } from '../../api/project'

vi.mock('../../api/project', () => ({
  initProject: vi.fn(),
}))
vi.mock('sonner', () => ({
  toast: { success: vi.fn(), error: vi.fn() },
}))

const initProjectMock = vi.mocked(initProject)

function renderPage() {
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false }, mutations: { retry: false } },
  })
  const invalidateSpy = vi.spyOn(queryClient, 'invalidateQueries')
  render(
    <QueryClientProvider client={queryClient}>
      <InitProjectPage />
    </QueryClientProvider>,
  )
  return { invalidateSpy }
}

beforeEach(() => {
  vi.clearAllMocks()
})

describe('InitProjectPage', () => {
  it('submits the form and shows the created files on success', async () => {
    const user = userEvent.setup()
    initProjectMock.mockResolvedValue({
      success: true,
      created_files: ['lhp.yaml', 'databricks.yml'],
      created_dirs: ['pipelines', 'presets'],
      bundle_enabled: true,
      error_message: null,
      error_code: null,
    })
    const { invalidateSpy } = renderPage()

    await user.type(screen.getByLabelText('Project name'), 'demo_project')
    await user.click(screen.getByRole('button', { name: /create project/i }))

    await waitFor(() =>
      expect(screen.getByText('Project initialized')).toBeInTheDocument(),
    )
    // mutationFn receives (variables, context) under TanStack Query v5 —
    // only the variables payload is under test here.
    expect(initProjectMock.mock.calls[0][0]).toEqual({
      project_name: 'demo_project',
      bundle: true,
    })
    expect(screen.getByText('lhp.yaml')).toBeInTheDocument()
    expect(screen.getByText('pipelines')).toBeInTheDocument()
    // The whole cache reloads so the app flips out of the no_project state.
    expect(invalidateSpy).toHaveBeenCalledWith()
  })

  it('sends bundle=false when the toggle is unchecked and null name when blank', async () => {
    const user = userEvent.setup()
    initProjectMock.mockResolvedValue({
      success: true,
      created_files: [],
      created_dirs: [],
      bundle_enabled: false,
      error_message: null,
      error_code: null,
    })
    renderPage()

    await user.click(screen.getByRole('checkbox'))
    await user.click(screen.getByRole('button', { name: /create project/i }))

    await waitFor(() => expect(initProjectMock).toHaveBeenCalled())
    expect(initProjectMock.mock.calls[0][0]).toEqual({
      project_name: null,
      bundle: false,
    })
  })

  it('shows the scaffolding error when the backend reports success=false', async () => {
    const user = userEvent.setup()
    initProjectMock.mockResolvedValue({
      success: false,
      created_files: [],
      created_dirs: [],
      bundle_enabled: false,
      error_message: 'Directory is not empty',
      error_code: 'LHP-IO-001',
    })
    renderPage()

    await user.click(screen.getByRole('button', { name: /create project/i }))

    await waitFor(() =>
      expect(screen.getByRole('alert')).toHaveTextContent('Directory is not empty'),
    )
    expect(screen.getByRole('alert')).toHaveTextContent('LHP-IO-001')
    // The form stays visible for a retry.
    expect(screen.getByRole('button', { name: /create project/i })).toBeInTheDocument()
  })

  it('shows a request error when the POST itself rejects', async () => {
    const user = userEvent.setup()
    initProjectMock.mockRejectedValue(new Error('conflict'))
    renderPage()

    await user.click(screen.getByRole('button', { name: /create project/i }))

    await waitFor(() => expect(screen.getByRole('alert')).toBeInTheDocument())
    expect(screen.getByRole('alert')).toHaveTextContent(
      'Project initialization failed unexpectedly.',
    )
  })
})
