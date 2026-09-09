import type { ReactNode } from 'react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { act, fireEvent, render, screen, waitFor } from '@testing-library/react'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import { previewTemplate, type TemplatePreviewResponse } from '@/api/template-authoring'
import { TooltipProvider } from '@/components/ui/tooltip'
import { useWorkspaceStore } from '@/store/workspaceStore'
import { useTemplatePreviewStore } from '@/store/templatePreviewStore'
import { TemplatePreview } from '../TemplatePreview'

vi.mock('@/api/template-authoring', () => ({ previewTemplate: vi.fn() }))
vi.mock('@/hooks/useEnvironments', () => ({
  useEnvironments: () => ({ data: { environments: ['dev', 'prod'] }, isError: false, refetch: vi.fn() }),
}))
vi.mock('@/workspace/openWorkspaceFile', () => ({ openWorkspaceFile: vi.fn() }))

const path = 'templates/sample.yaml'
const project = 'template-preview-components'
const source = 'name: sample\nparameters:\n  - name: columns\n    type: array\n    required: true\nactions: []\n'
const ready: TemplatePreviewResponse = {
  request_revision: '1', source_hash: 'hash', stage: 'expanded', status: 'ready',
  diagnostics: [], missing_parameters: [], expanded_actions: [], saved_dependencies: [],
}

function wrapper({ children }: { children: ReactNode }) {
  return <QueryClientProvider client={new QueryClient({ defaultOptions: { queries: { retry: false } } })}>
    <TooltipProvider>{children}</TooltipProvider>
  </QueryClientProvider>
}

beforeEach(() => {
  vi.clearAllMocks()
  Element.prototype.scrollIntoView = vi.fn()
  useWorkspaceStore.getState().closeAllBuffers()
  useWorkspaceStore.getState().ensureProjectScope(project)
  useWorkspaceStore.getState().openBuffer(path, { content: source, exists: true })
  useWorkspaceStore.getState().openEntityTab('', 'sample', path, { docKind: 'template', view: 'preview' })
  useTemplatePreviewStore.setState({ sessions: {}, dependenciesRevision: 0 })
  vi.mocked(previewTemplate).mockResolvedValue(ready)
})

describe('template preview inputs and results', () => {
  it('marks a checked result as previous while invalid sample text remains uncommitted', async () => {
    render(<TemplatePreview path={path} tabId="preview-tab" />, { wrapper })
    fireEvent.click(screen.getByRole('button', { name: 'Supply value' }))
    fireEvent.click(screen.getByRole('button', { name: 'Preview draft' }))
    await screen.findByText('Expanded actions checked')
    fireEvent.click(screen.getByRole('button', { name: 'Edit Value for columns as YAML' }))
    const sample = screen.getByLabelText('Value for columns')
    fireEvent.change(sample, { target: { value: '[unclosed' } })

    expect(screen.getByText('Previous result')).toBeInTheDocument()
    expect(screen.getByText('Preview is out of date. Refresh to check current inputs.')).toBeInTheDocument()
    expect(screen.getByRole('button', { name: 'Refresh preview' })).toBeDisabled()
    expect(screen.queryByText('Expanded actions checked')).not.toBeInTheDocument()
    fireEvent.blur(sample)
    expect(previewTemplate).toHaveBeenCalledTimes(1)
    expect(useTemplatePreviewStore.getState().sessions[`${project}::${path}`].values).toEqual({ columns: [] })

    fireEvent.keyDown(sample, { key: 'Escape' })
    expect(screen.getByRole('button', { name: 'Refresh preview' })).toBeEnabled()
    expect(screen.getByText('Expanded actions checked')).toBeInTheDocument()
  })

  it('marks valid focused sample edits stale and flushes the latest value when refreshing', async () => {
    useWorkspaceStore.getState().updateContent(path, source.replace('name: columns', 'name: count').replace('type: array', 'type: number'))
    render(<TemplatePreview path={path} tabId="preview-tab" />, { wrapper })
    fireEvent.click(screen.getByRole('button', { name: 'Supply value' }))
    fireEvent.click(screen.getByRole('button', { name: 'Preview draft' }))
    await screen.findByText('Expanded actions checked')
    const sample = screen.getByLabelText('Value for count')
    act(() => sample.focus())
    fireEvent.change(sample, { target: { value: '42' } })

    expect(sample).toHaveFocus()
    expect(sample).toHaveValue('42')
    expect(screen.getByText('Previous result')).toBeInTheDocument()
    expect(screen.queryByText('Expanded actions checked')).not.toBeInTheDocument()
    expect(screen.getByRole('button', { name: 'Refresh preview' })).toBeEnabled()
    expect(useTemplatePreviewStore.getState().sessions[`${project}::${path}`].values).toEqual({ count: 0 })

    // Keep the input focused until the command runs: it must capture the pending draft.
    fireEvent.click(screen.getByRole('button', { name: 'Refresh preview' }))
    await screen.findByText('Expanded actions checked')
    expect(previewTemplate).toHaveBeenCalledTimes(2)
    expect(previewTemplate).toHaveBeenLastCalledWith(expect.objectContaining({ sample_parameters: { count: 42 } }), expect.any(AbortSignal))
    expect(useTemplatePreviewStore.getState().sessions[`${project}::${path}`].values).toEqual({ count: 42 })
    expect(screen.queryByText('Previous result')).not.toBeInTheDocument()
  })

  it('moves focus to the sample parameter named by the backend missing-value diagnostic', async () => {
    // Mirrors template_preview.py: required samples use this code and declaration index.
    vi.mocked(previewTemplate).mockResolvedValue({
      ...ready, status: 'needs_parameters', missing_parameters: ['columns'], expanded_actions: null,
      diagnostics: [{
        code: 'LHP-TEMPLATE-SAMPLE', stage: 'expanded', severity: 'info', source_path: path,
        message: "Supply a sample value for required parameter 'columns'.", field_path: ['parameters', 0],
      }],
    })
    render(<TemplatePreview path={path} tabId="preview-tab" />, { wrapper })
    fireEvent.click(screen.getByRole('button', { name: 'Preview draft' }))
    await screen.findByText('Supply required parameters: columns')
    fireEvent.click(screen.getByRole('button', { name: 'Go to sample parameter' }))

    expect(screen.getByRole('region', { name: 'Parameter columns' })).toHaveFocus()
    expect(Element.prototype.scrollIntoView).toHaveBeenCalledWith({ block: 'nearest' })
    expect(screen.getByRole('button', { name: 'Supply value' })).toBeEnabled()
    expect(screen.getByRole('button', { name: 'Open source' })).toBeInTheDocument()
  })

  it('retains resolved context and sends additional presets and runtime variables with the draft', async () => {
    vi.mocked(previewTemplate).mockResolvedValue({ ...ready, stage: 'resolved', resolved_flowgroup: { actions: [] } })
    render(<TemplatePreview path={path} tabId="preview-tab" />, { wrapper })
    fireEvent.click(screen.getByRole('button', { name: 'Supply value' }))
    fireEvent.change(screen.getByLabelText('Preview level'), { target: { value: 'resolved' } })
    fireEvent.change(screen.getByLabelText('Sample pipeline'), { target: { value: 'sales' } })
    fireEvent.change(screen.getByLabelText('Sample flowgroup'), { target: { value: 'customers' } })
    fireEvent.change(screen.getByLabelText('Preview environment'), { target: { value: 'dev' } })
    fireEvent.click(screen.getByText('Additional presets and runtime variables'))
    fireEvent.change(screen.getByLabelText('Additional presets'), { target: { value: 'bronze_defaults' } })
    fireEvent.click(screen.getByRole('button', { name: 'Add Additional presets item' }))
    fireEvent.change(screen.getByLabelText('New Runtime variables key'), { target: { value: 'region' } })
    fireEvent.change(screen.getByLabelText('New Runtime variables value'), { target: { value: 'ap-southeast-2' } })
    fireEvent.click(screen.getByRole('button', { name: 'Add Runtime variables entry' }))
    fireEvent.click(screen.getByRole('button', { name: 'Preview draft' }))

    const context = { pipeline: 'sales', flowgroup: 'customers', environment: 'dev', presets: ['bronze_defaults'], variables: { region: 'ap-southeast-2' } }
    await waitFor(() => expect(previewTemplate).toHaveBeenCalledWith(expect.objectContaining({
      source_path: path, source_yaml: source, stage: 'resolved', sample_parameters: { columns: [] }, context,
    }), expect.any(AbortSignal)))
    await screen.findByText('Resolved flowgroup checked')
    expect(useTemplatePreviewStore.getState().sessions[`${project}::${path}`].context).toEqual(context)
    expect(useWorkspaceStore.getState().buffers[0].content).toBe(source)
    expect(useWorkspaceStore.getState().buffers[0].isDirty).toBe(false)
  })
})
