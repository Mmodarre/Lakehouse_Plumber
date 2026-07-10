import { beforeEach, describe, expect, it, vi } from 'vitest'
import type { ReactNode } from 'react'
import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'

vi.mock('../../../api/config-templates', () => ({
  fetchConfigTemplate: vi.fn(),
}))
vi.mock('../../../api/files', async (importOriginal) => ({
  // Keep IF_MATCH_CREATE_ONLY (and friends) real; stub only the writer.
  ...(await importOriginal<typeof import('../../../api/files')>()),
  writeFile: vi.fn(),
}))
vi.mock('sonner', () => ({
  toast: { error: vi.fn(), success: vi.fn(), dismiss: vi.fn() },
}))

import { CreateFromTemplateDialog } from '../CreateFromTemplateDialog'
import { fetchConfigTemplate } from '../../../api/config-templates'
import { IF_MATCH_CREATE_ONLY, writeFile } from '../../../api/files'
import { ApiError } from '../../../api/client'
import type { FileNode } from '../../../types/api'

const mockFetchTemplate = vi.mocked(fetchConfigTemplate)
const mockWriteFile = vi.mocked(writeFile)

const tree: FileNode = {
  name: '',
  path: '',
  type: 'directory',
  children: [
    {
      name: 'config',
      path: 'config',
      type: 'directory',
      children: [
        { name: 'pipeline_config_dev.yaml', path: 'config/pipeline_config_dev.yaml', type: 'file' },
      ],
    },
  ],
}

function renderDialog(overrides: Partial<Parameters<typeof CreateFromTemplateDialog>[0]> = {}) {
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false, staleTime: Infinity } },
  })
  queryClient.setQueryData(['files'], tree)
  const invalidateSpy = vi.spyOn(queryClient, 'invalidateQueries')
  const props = {
    kind: 'pipeline' as const,
    open: true,
    onOpenChange: vi.fn(),
    onCreated: vi.fn(),
    ...overrides,
  }
  const wrapper = ({ children }: { children: ReactNode }) => (
    <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
  )
  render(<CreateFromTemplateDialog {...props} />, { wrapper })
  return { props, invalidateSpy }
}

describe('CreateFromTemplateDialog', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    localStorage.clear()
    mockFetchTemplate.mockResolvedValue('# template body\n')
    mockWriteFile.mockResolvedValue({ written: true, path: 'x', yaml_error: null, etag: 'e1' })
  })

  it('renders nothing while closed', () => {
    renderDialog({ open: false })
    expect(screen.queryByText(/new config file/i)).not.toBeInTheDocument()
  })

  it('fetches the template and PUTs it create-only, then selects the new file', async () => {
    const user = userEvent.setup()
    const { props, invalidateSpy } = renderDialog()

    // The filename defaults to the env-suffixed template path; the default
    // env is 'dev', and pipeline_config_dev.yaml exists — pick another name.
    const input = screen.getByLabelText(/file name/i)
    await user.clear(input)
    await user.type(input, 'config/pipeline_config_prod.yaml')
    await user.click(screen.getByRole('button', { name: /create/i }))

    await waitFor(() => expect(props.onCreated).toHaveBeenCalledWith('config/pipeline_config_prod.yaml'))
    expect(mockFetchTemplate).toHaveBeenCalledExactlyOnceWith('pipeline_config')
    expect(mockWriteFile).toHaveBeenCalledExactlyOnceWith(
      'config/pipeline_config_prod.yaml',
      '# template body\n',
      IF_MATCH_CREATE_ONLY,
    )
    expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: ['files'] })
    expect(props.onOpenChange).toHaveBeenCalledWith(false)
  })

  it('refuses a filename that already exists in the tree (no write attempted)', async () => {
    const user = userEvent.setup()
    renderDialog()

    const input = screen.getByLabelText(/file name/i)
    await user.clear(input)
    await user.type(input, 'config/pipeline_config_dev.yaml')

    expect(screen.getByText('This file already exists')).toBeInTheDocument()
    expect(screen.getByRole('button', { name: /create/i })).toBeDisabled()
    expect(mockWriteFile).not.toHaveBeenCalled()
  })

  it('validates the config/ prefix and .yaml suffix', async () => {
    const user = userEvent.setup()
    renderDialog()

    const input = screen.getByLabelText(/file name/i)
    await user.clear(input)
    await user.type(input, 'pipeline_config_prod.yaml')
    expect(screen.getByText('The file must live under config/')).toBeInTheDocument()

    await user.clear(input)
    await user.type(input, 'config/pipeline_config_prod.txt')
    expect(screen.getByText('The file name must end with .yaml')).toBeInTheDocument()
    expect(screen.getByRole('button', { name: /create/i })).toBeDisabled()
  })

  it('surfaces a 412 as "already exists" and keeps the dialog open', async () => {
    const user = userEvent.setup()
    mockWriteFile.mockRejectedValueOnce(
      new ApiError(412, {
        code: 'PRECONDITION_FAILED',
        category: 'io',
        message: 'stale',
        details: '',
        suggestions: [],
        context: {},
        http_status: 412,
      }),
    )
    const { props } = renderDialog()

    const input = screen.getByLabelText(/file name/i)
    await user.clear(input)
    await user.type(input, 'config/pipeline_config_prod.yaml')
    await user.click(screen.getByRole('button', { name: /create/i }))

    expect(await screen.findByText('This file already exists on disk.')).toBeInTheDocument()
    expect(props.onCreated).not.toHaveBeenCalled()
    expect(props.onOpenChange).not.toHaveBeenCalledWith(false)
  })
})
