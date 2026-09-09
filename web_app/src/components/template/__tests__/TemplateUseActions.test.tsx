import { act, render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import { fetchTemplateSource, type TemplateCatalogEntry } from '@/api/template-authoring'
import { useWorkspaceStore } from '@/store/workspaceStore'
import { useLayoutStore } from '@/store/layoutStore'
import { useUIStore } from '@/store/uiStore'
import { TemplateUseActions } from '../TemplateUseActions'

vi.mock('@/api/template-authoring', () => ({ fetchTemplateSource: vi.fn() }))
const path = 'templates/nested/example.yaml'
const entry: TemplateCatalogEntry = { source_path: path, reference: 'nested/example', declared_name: 'Example', description: null, version: null, state: 'ready', parameters: [], presets: [], action_count: 1, diagnostics: [] }
beforeEach(() => {
  vi.clearAllMocks()
  useWorkspaceStore.getState().closeAllBuffers()
  useWorkspaceStore.getState().ensureProjectScope('use-project')
  useWorkspaceStore.getState().openBuffer(path, { content: 'name: Example\nactions: []\n', exists: true })
  useWorkspaceStore.getState().openEntityTab('', 'Example', path, { docKind: 'template' })
  useLayoutStore.setState({ viewerMode: false })
})
function setup() {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } })
  const open = vi.spyOn(useUIStore.getState(), 'openCreateFlowgroupDialog')
  const save = vi.fn().mockResolvedValue(true)
  const rendered = render(<QueryClientProvider client={client}><TemplateUseActions path={path} onSave={save} /></QueryClientProvider>)
  return { ...rendered, client, open, save }
}
describe('Use template lifecycle', () => {
  it('passes the full source path to Create flowgroup after validating saved metadata', async () => {
    vi.mocked(fetchTemplateSource).mockResolvedValue({ template: entry })
    const { open, save } = setup()
    await userEvent.click(screen.getByRole('button', { name: 'Use template' }))
    await waitFor(() => expect(open).toHaveBeenCalledWith({ templatePath: path }))
    expect(save).not.toHaveBeenCalled()
  })
  it.each(['edit', 'viewer', 'unmount'] as const)('does not open creation after %s during metadata loading', async (change) => {
    let finish!: (value: { template: TemplateCatalogEntry }) => void
    vi.mocked(fetchTemplateSource).mockImplementation(() => new Promise((resolve) => { finish = resolve }))
    const { open, unmount } = setup()
    await userEvent.click(screen.getByRole('button', { name: 'Use template' }))
    act(() => {
      if (change === 'edit') useWorkspaceStore.getState().updateContent(path, 'name: Changed\nactions: []\n')
      if (change === 'viewer') useLayoutStore.setState({ viewerMode: true })
      if (change === 'unmount') unmount()
    })
    await act(async () => { finish({ template: entry }); await Promise.resolve() })
    expect(open).not.toHaveBeenCalled()
  })
  it('does not open creation when an edit arrives during catalog refresh', async () => {
    vi.mocked(fetchTemplateSource).mockResolvedValue({ template: entry })
    const { open, client } = setup()
    let finish!: () => void
    vi.spyOn(client, 'invalidateQueries').mockImplementation(() => new Promise((resolve) => { finish = resolve }))
    await userEvent.click(screen.getByRole('button', { name: 'Use template' }))
    await waitFor(() => expect(client.invalidateQueries).toHaveBeenCalled())
    act(() => useWorkspaceStore.getState().updateContent(path, 'name: Changed\nactions: []\n'))
    await act(async () => { finish(); await Promise.resolve() })
    expect(open).not.toHaveBeenCalled()
  })
})
