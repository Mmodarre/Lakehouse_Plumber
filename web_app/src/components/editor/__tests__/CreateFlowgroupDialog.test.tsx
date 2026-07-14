import { beforeEach, describe, expect, it, vi } from 'vitest'
import type { ReactNode } from 'react'
import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'

vi.mock('../../../api/files', async (importOriginal) => ({
  // Keep IF_MATCH_CREATE_ONLY real; stub the writer + tree fetch.
  ...(await importOriginal<typeof import('../../../api/files')>()),
  writeFile: vi.fn(),
  fetchFiles: vi.fn(),
}))
vi.mock('../../../api/pipelines', () => ({
  fetchPipelines: vi.fn().mockResolvedValue({ pipelines: [], total: 0 }),
  fetchPipelineDetail: vi.fn(),
  fetchPipelineFlowgroups: vi.fn(),
}))
vi.mock('../../../api/flowgroups', () => ({
  fetchFlowgroups: vi.fn().mockResolvedValue({ flowgroups: [], total: 0 }),
  fetchFlowgroupDetail: vi.fn(),
  fetchFlowgroupRelatedFiles: vi.fn(),
  fetchFlowgroupResolved: vi.fn(),
}))
vi.mock('../../../api/templates', () => ({
  fetchTemplates: vi.fn().mockResolvedValue({ templates: [], total: 0 }),
  fetchTemplateDetail: vi.fn(),
}))
vi.mock('../../../api/blueprints', () => ({
  fetchBlueprints: vi.fn().mockResolvedValue({ blueprints: [], total: 0 }),
}))
vi.mock('sonner', () => ({ toast: { error: vi.fn(), success: vi.fn(), dismiss: vi.fn() } }))

import { CreateFlowgroupDialog } from '../CreateFlowgroupDialog'
import { IF_MATCH_CREATE_ONLY, writeFile } from '../../../api/files'
import { ApiError } from '../../../api/client'
import { useUIStore } from '../../../store/uiStore'
import { useWorkspaceStore } from '../../../store/workspaceStore'
import type { FileNode } from '../../../types/api'

const mockWriteFile = vi.mocked(writeFile)

const tree: FileNode = {
  name: '',
  path: '',
  type: 'directory',
  children: [
    {
      name: 'pipelines',
      path: 'pipelines',
      type: 'directory',
      children: [{ name: 'sales_raw', path: 'pipelines/sales_raw', type: 'directory', children: [] }],
    },
  ],
}

function setup() {
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false, staleTime: Infinity } },
  })
  // Warm the caches so the form initialises with data synchronously.
  queryClient.setQueryData(['pipelines'], {
    pipelines: [{ name: 'sales_raw', flowgroup_count: 1, action_count: 1 }],
    total: 1,
  })
  queryClient.setQueryData(['flowgroups', undefined], {
    flowgroups: [{ name: 'orders', pipeline: 'sales_raw' }],
    total: 1,
  })
  queryClient.setQueryData(['files'], tree)
  queryClient.setQueryData(['templates'], { templates: [], total: 0 })
  queryClient.setQueryData(['blueprints', false], { blueprints: [], total: 0 })
  const invalidateSpy = vi.spyOn(queryClient, 'invalidateQueries')

  const openEntityTab = vi.fn()
  useWorkspaceStore.setState({ openEntityTab })
  useUIStore.setState({ createFlowgroupDialog: true, createFlowgroupSeed: null })

  const wrapper = ({ children }: { children: ReactNode }) => (
    <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
  )
  render(<CreateFlowgroupDialog />, { wrapper })
  return { openEntityTab, invalidateSpy }
}

const nameInput = () => screen.getByPlaceholderText('e.g. customer_orders')
const createButton = () => screen.getByRole('button', { name: /^create$/i })

beforeEach(() => {
  vi.clearAllMocks()
  localStorage.clear()
  mockWriteFile.mockResolvedValue({ written: true, path: 'x', yaml_error: null, etag: 'e1' })
})

describe('CreateFlowgroupDialog', () => {
  it('disables Create until a valid flowgroup name is entered', () => {
    setup()
    expect(createButton()).toBeDisabled()
  })

  it('flags a same-pipeline collision and blocks Create', async () => {
    const user = userEvent.setup()
    setup()
    // Default pipeline is sales_raw, where "orders" already lives.
    await user.type(nameInput(), 'orders')
    expect(screen.getByText(/already exists in this pipeline/i)).toBeInTheDocument()
    expect(createButton()).toBeDisabled()
    expect(mockWriteFile).not.toHaveBeenCalled()
  })

  it('allows a name that exists in another pipeline (per-pipeline uniqueness, I-2)', async () => {
    const user = userEvent.setup()
    setup()
    // "orders" exists only in sales_raw; target a different (new) pipeline.
    await user.click(screen.getByRole('button', { name: 'New pipeline' }))
    await user.type(screen.getByPlaceholderText('New pipeline name'), 'other_pipe')
    await user.type(nameInput(), 'orders')
    expect(screen.queryByText(/already exists/i)).not.toBeInTheDocument()
    expect(screen.getByText('pipelines/other_pipe/orders.yaml')).toBeInTheDocument()
    expect(createButton()).toBeEnabled()
  })

  it('supports a new pipeline and previews the derived path', async () => {
    const user = userEvent.setup()
    setup()
    await user.click(screen.getByRole('button', { name: 'New pipeline' }))
    await user.type(screen.getByPlaceholderText('New pipeline name'), 'analytics')
    await user.type(nameInput(), 'metrics')
    expect(screen.getByText('pipelines/analytics/metrics.yaml')).toBeInTheDocument()
    expect(createButton()).toBeEnabled()
  })

  it('creates a blank flowgroup create-only and opens the designer on it', async () => {
    const user = userEvent.setup()
    const { openEntityTab, invalidateSpy } = setup()
    await user.type(nameInput(), 'new_fg')
    await user.click(createButton())

    await waitFor(() => expect(mockWriteFile).toHaveBeenCalledTimes(1))
    const [path, content, etag] = mockWriteFile.mock.calls[0]!
    expect(path).toBe('pipelines/sales_raw/new_fg.yaml')
    expect(content).toContain('flowgroup: new_fg')
    expect(content).toContain('pipeline: sales_raw')
    expect(etag).toBe(IF_MATCH_CREATE_ONLY)

    // No explicit view override — the new flowgroup opens in the default Graph view.
    await waitFor(() =>
      expect(openEntityTab).toHaveBeenCalledWith(
        'sales_raw',
        'new_fg',
        'pipelines/sales_raw/new_fg.yaml',
      ),
    )
    expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: ['files'] })
    expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: ['pipelines'] })
    expect(useUIStore.getState().createFlowgroupDialog).toBe(false)
  })

  it('turns a 412 into an overwrite confirmation, then writes unconditionally', async () => {
    const user = userEvent.setup()
    const { openEntityTab } = setup()
    mockWriteFile.mockRejectedValueOnce(
      new ApiError(412, {
        code: 'PRECONDITION_FAILED',
        category: 'io',
        message: 'exists',
        details: '',
        suggestions: [],
        context: {},
        http_status: 412,
      }),
    )
    await user.type(nameInput(), 'new_fg')
    await user.click(createButton())

    expect(await screen.findByText(/already exists at/i)).toBeInTheDocument()
    const overwrite = screen.getByRole('button', { name: /overwrite/i })
    await user.click(overwrite)

    await waitFor(() => expect(mockWriteFile).toHaveBeenCalledTimes(2))
    // The overwrite PUT is unconditional (no create-only etag).
    expect(mockWriteFile.mock.calls[1]![2]).toBeUndefined()
    await waitFor(() => expect(openEntityTab).toHaveBeenCalled())
  })

  it('after a 412, editing the name returns a Create targeting the new path (I-1)', async () => {
    const user = userEvent.setup()
    setup()
    mockWriteFile.mockRejectedValueOnce(
      new ApiError(412, {
        code: 'PRECONDITION_FAILED',
        category: 'io',
        message: 'exists',
        details: '',
        suggestions: [],
        context: {},
        http_status: 412,
      }),
    )
    await user.type(nameInput(), 'new_fg')
    await user.click(createButton())
    expect(await screen.findByRole('button', { name: /overwrite/i })).toBeInTheDocument()

    // Editing the name drops the stale overwrite → the footer returns to Create.
    await user.type(nameInput(), '2') // → new_fg2
    await waitFor(() =>
      expect(screen.queryByRole('button', { name: /overwrite/i })).not.toBeInTheDocument(),
    )
    expect(createButton()).toBeEnabled()

    // Creating now writes the EDITED path, create-only — never the stale one.
    await user.click(createButton())
    await waitFor(() => expect(mockWriteFile).toHaveBeenCalledTimes(2))
    const [path, , etag] = mockWriteFile.mock.calls[1]!
    expect(path).toBe('pipelines/sales_raw/new_fg2.yaml')
    expect(etag).toBe(IF_MATCH_CREATE_ONLY)
  })
})
