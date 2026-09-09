import { beforeEach, describe, expect, it, vi } from 'vitest'
import type { ReactNode } from 'react'
import { act, render, screen, waitFor } from '@testing-library/react'
import { parse as parseYaml } from 'yaml'
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
vi.mock('../../../api/template-authoring', () => ({ fetchTemplateCatalog: vi.fn().mockResolvedValue({ templates: [], total: 0 }), fetchTemplateSource: vi.fn() }))
vi.mock('../../../api/blueprints', () => ({
  fetchBlueprints: vi.fn().mockResolvedValue({ blueprints: [], total: 0 }),
}))
vi.mock('../../../workspace/editorCommands', () => ({ captureWorkspaceEditors: vi.fn(), focusInvalidWorkspaceDraft: vi.fn().mockReturnValue(false) }))
vi.mock('sonner', () => ({ toast: { error: vi.fn(), success: vi.fn(), dismiss: vi.fn() } }))

import { CreateFlowgroupDialog } from '../CreateFlowgroupDialog'
import { IF_MATCH_CREATE_ONLY, writeFile } from '../../../api/files'
import { ApiError } from '../../../api/client'
import { useUIStore } from '../../../store/uiStore'
import { useLayoutStore } from '../../../store/layoutStore'
import { captureWorkspaceEditors } from '../../../workspace/editorCommands'
import { fetchTemplateSource, type TemplateCatalogEntry } from '../../../api/template-authoring'
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

function setup(options: { template?: TemplateCatalogEntry; seedTemplatePath?: string; cachedMetadata?: TemplateCatalogEntry } = {}) {
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
  queryClient.setQueryData(['templates', 'catalog'], { templates: options.template ? [options.template] : [], total: options.template ? 1 : 0 })
  queryClient.setQueryData(['blueprints', false], { blueprints: [], total: 0 })
  if (options.cachedMetadata) queryClient.setQueryData(['template', options.cachedMetadata.source_path], { template: options.cachedMetadata })
  const invalidateSpy = vi.spyOn(queryClient, 'invalidateQueries')

  const openEntityTab = vi.fn()
  useWorkspaceStore.setState({ openEntityTab })
  useUIStore.setState({ createFlowgroupDialog: true, createFlowgroupSeed: options.seedTemplatePath ? { templatePath: options.seedTemplatePath } : null })

  const wrapper = ({ children }: { children: ReactNode }) => (
    <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
  )
  render(<CreateFlowgroupDialog />, { wrapper })
  return { openEntityTab, invalidateSpy, queryClient }
}

const nameInput = () => screen.getByPlaceholderText('e.g. customer_orders')
const createButton = () => screen.getByRole('button', { name: /^create$/i })

beforeEach(() => {
  vi.clearAllMocks()
  localStorage.clear()
  useLayoutStore.setState({ viewerMode: false })
  vi.mocked(fetchTemplateSource).mockReset()
  vi.mocked(captureWorkspaceEditors).mockReset()
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

const reusableTemplate: TemplateCatalogEntry = {
  source_path: 'templates/ingestion/reusable.yaml', reference: 'ingestion/reusable',
  declared_name: 'Different display name', version: '1.0', description: null,
  state: 'ready', parameters: [{ name: 'limit', required: true, has_default: true, default: 0, declared_type: 'number', description: null }],
  presets: [], action_count: 2, diagnostics: [],
}

function existingFileError() {
  return new ApiError(412, { code: 'PRECONDITION_FAILED', category: 'io', message: 'exists', details: '', suggestions: [], context: {}, http_status: 412 })
}

describe('creation safety and template metadata', () => {
  it('disables creation fields in viewer mode and retains a working Cancel', () => {
    useLayoutStore.setState({ viewerMode: true })
    setup()
    expect(nameInput()).toBeDisabled()
    expect(createButton()).toBeDisabled()
    expect(screen.getByRole('button', { name: 'Cancel' })).toBeEnabled()
    expect(screen.getByText(/viewer mode: creation and overwrite are disabled/i)).toBeInTheDocument()
    expect(mockWriteFile).not.toHaveBeenCalled()
  })

  it('checks the live viewer mode after capturing editor drafts, before persisting', async () => {
    const user = userEvent.setup()
    setup()
    await user.type(nameInput(), 'viewer_race')
    expect(createButton()).toBeEnabled()
    vi.mocked(captureWorkspaceEditors).mockImplementationOnce(() => useLayoutStore.setState({ viewerMode: true }))
    await user.click(createButton())
    expect(mockWriteFile).not.toHaveBeenCalled()
    expect(useUIStore.getState().createFlowgroupDialog).toBe(true)
    expect(createButton()).toBeDisabled()
  })

  it('blocks a pending overwrite when viewer mode is enabled', async () => {
    const user = userEvent.setup()
    setup()
    mockWriteFile.mockRejectedValueOnce(existingFileError())
    await user.type(nameInput(), 'existing_file')
    await user.click(createButton())
    const overwrite = await screen.findByRole('button', { name: 'Overwrite' })
    act(() => useLayoutStore.setState({ viewerMode: true }))
    expect(overwrite).toBeDisabled()
    await user.click(overwrite)
    expect(mockWriteFile).toHaveBeenCalledTimes(1)
    expect(nameInput()).toBeDisabled()
  })

  it('keeps the creation operation visible when Escape is pressed during a write', async () => {
    const user = userEvent.setup()
    let finish!: (value: Awaited<ReturnType<typeof writeFile>>) => void
    mockWriteFile.mockImplementationOnce(() => new Promise((resolve) => { finish = resolve }))
    const { openEntityTab } = setup()
    await user.type(nameInput(), 'pending_file')
    await user.click(createButton())
    expect(mockWriteFile).toHaveBeenCalledTimes(1)
    await user.keyboard('{Escape}')
    expect(screen.getByRole('dialog')).toBeInTheDocument()
    expect(useUIStore.getState().createFlowgroupDialog).toBe(true)
    expect(screen.getByRole('button', { name: 'Close' })).toBeDisabled()
    expect(screen.getByRole('button', { name: 'Cancel' })).toBeDisabled()
    expect(nameInput()).toBeDisabled()
    await act(async () => finish({ written: true, path: 'pipelines/sales_raw/pending_file.yaml', yaml_error: null, etag: 'saved' }))
    await waitFor(() => expect(openEntityTab).toHaveBeenCalled())
    expect(useUIStore.getState().createFlowgroupDialog).toBe(false)
  })

  it('waits for template metadata and explicitly supplies a required default using the nested reference', async () => {
    const user = userEvent.setup()
    let finish!: (value: { template: TemplateCatalogEntry }) => void
    vi.mocked(fetchTemplateSource).mockImplementationOnce(() => new Promise((resolve) => { finish = resolve }))
    setup({ template: reusableTemplate, seedTemplatePath: reusableTemplate.source_path })
    await user.type(nameInput(), 'templated_orders')
    expect(screen.getByText('Reading template parameters…')).toBeInTheDocument()
    expect(createButton()).toBeDisabled()
    await act(async () => finish({ template: reusableTemplate }))
    const supplyDefault = await screen.findByRole('button', { name: 'Supply declared default' })
    expect(createButton()).toBeDisabled()
    await user.click(supplyDefault)
    expect(createButton()).toBeEnabled()
    await user.click(createButton())
    await waitFor(() => expect(mockWriteFile).toHaveBeenCalledTimes(1))
    const document = parseYaml(mockWriteFile.mock.calls[0]![1])
    expect(document.use_template).toBe('ingestion/reusable')
    expect(document.use_template).not.toBe(reusableTemplate.declared_name)
    expect(document.template_parameters).toEqual({ limit: 0 })
  })

  it('disables creation while cached ready template metadata is being refreshed', async () => {
    const user = userEvent.setup()
    let finish!: (value: { template: TemplateCatalogEntry }) => void
    vi.mocked(fetchTemplateSource).mockImplementationOnce(() => new Promise((resolve) => { finish = resolve }))
    const { queryClient } = setup({ template: reusableTemplate, seedTemplatePath: reusableTemplate.source_path, cachedMetadata: reusableTemplate })
    await user.type(nameInput(), 'cached_template')
    await user.click(screen.getByRole('button', { name: 'Supply declared default' }))
    expect(createButton()).toBeEnabled()
    let refetch!: Promise<void>
    act(() => { refetch = queryClient.invalidateQueries({ queryKey: ['template', reusableTemplate.source_path] }) })
    await waitFor(() => expect(createButton()).toBeDisabled())
    expect(queryClient.getQueryData(['template', reusableTemplate.source_path])).toEqual({ template: reusableTemplate })
    expect(screen.getByText('Reading template parameters…')).toBeInTheDocument()
    await user.click(createButton())
    expect(mockWriteFile).not.toHaveBeenCalled()
    await act(async () => { finish({ template: reusableTemplate }); await refetch })
    await waitFor(() => expect(createButton()).toBeEnabled())
  })

  it('checks live metadata refetch state after capturing editors in the submit handler', async () => {
    const user = userEvent.setup()
    let finish!: (value: { template: TemplateCatalogEntry }) => void
    vi.mocked(fetchTemplateSource).mockImplementationOnce(() => new Promise((resolve) => { finish = resolve }))
    const { queryClient } = setup({ template: reusableTemplate, seedTemplatePath: reusableTemplate.source_path, cachedMetadata: reusableTemplate })
    await user.type(nameInput(), 'metadata_race')
    await user.click(screen.getByRole('button', { name: 'Supply declared default' }))
    expect(createButton()).toBeEnabled()
    vi.mocked(captureWorkspaceEditors).mockImplementationOnce(() => {
      void queryClient.refetchQueries({ queryKey: ['template', reusableTemplate.source_path] })
    })
    await user.click(createButton())
    expect(mockWriteFile).not.toHaveBeenCalled()
    expect(createButton()).toBeDisabled()
    await act(async () => finish({ template: reusableTemplate }))
    await waitFor(() => expect(createButton()).toBeEnabled())
  })

  it('blocks creation when template metadata fails and exposes a retry', async () => {
    const user = userEvent.setup()
    vi.mocked(fetchTemplateSource).mockRejectedValueOnce(new Error('Cannot read source'))
    setup({ template: reusableTemplate, seedTemplatePath: reusableTemplate.source_path })
    await user.type(nameInput(), 'templated_orders')
    expect(await screen.findByText(/could not read this template/i)).toBeInTheDocument()
    expect(screen.getByRole('button', { name: 'Retry template' })).toBeEnabled()
    expect(createButton()).toBeDisabled()
    await user.click(createButton())
    expect(mockWriteFile).not.toHaveBeenCalled()
  })
})
