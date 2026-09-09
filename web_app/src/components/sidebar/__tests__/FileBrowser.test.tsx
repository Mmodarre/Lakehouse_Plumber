import { beforeEach, describe, expect, it, vi } from 'vitest'
import { fireEvent, render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { FileBrowser } from '../FileBrowser'
import { writeFile, fetchFileContentWithMeta } from '@/api/files'
import { useLayoutStore } from '@/store/layoutStore'
import { useWorkspaceStore } from '@/store/workspaceStore'
import { ApiError } from '@/api/client'
import { toast } from 'sonner'

vi.mock('@/hooks/useFiles', () => ({ useFileList: () => ({ data: { name: '', path: '', type: 'directory', children: [{ name: 'existing.sql', path: 'existing.sql', type: 'file' }] }, isLoading: false }) }))
vi.mock('@/hooks/useFlowgroups', () => ({ useFlowgroups: () => ({ data: { flowgroups: [] } }) }))
vi.mock('../../sandbox/useSandboxScope', () => ({ useSandboxScope: () => null }))
vi.mock('@/api/files', () => ({ writeFile: vi.fn(), deleteFile: vi.fn(), fetchFileContentWithMeta: vi.fn(), IF_MATCH_CREATE_ONLY: 'create-only' }))
vi.mock('sonner', () => ({ toast: { error: vi.fn(), success: vi.fn() } }))

function renderFiles() {
  return render(<QueryClientProvider client={new QueryClient({ defaultOptions: { queries: { retry: false } } })}><FileBrowser /></QueryClientProvider>)
}

beforeEach(() => {
  vi.resetAllMocks()
  useWorkspaceStore.getState().closeAllBuffers()
  useLayoutStore.getState().setViewerMode(false)
})

describe('file creation safety and contextual commands', () => {
  it('uses an atomic create-only write and offers Open on collision', async () => {
    vi.mocked(writeFile).mockRejectedValue(new ApiError(412, { code: 'STALE', category: 'io', message: 'already exists', details: '', suggestions: [], context: {}, http_status: 412 }))
    renderFiles()
    await userEvent.click(screen.getByRole('button', { name: 'New file' }))
    fireEvent.change(screen.getByLabelText('New file path'), { target: { value: 'existing.sql' } })
    fireEvent.keyDown(screen.getByLabelText('New file path'), { key: 'Enter' })
    await waitFor(() => expect(writeFile).toHaveBeenCalledWith('existing.sql', '', 'create-only'))
    await waitFor(() => expect(toast.error).toHaveBeenCalledWith('File already exists', expect.objectContaining({ action: expect.objectContaining({ label: 'Open file' }) })))
    expect(useWorkspaceStore.getState().buffers).toEqual([])
  })

  it('duplicates current unsaved content with create-only semantics', async () => {
    useWorkspaceStore.getState().openBuffer('existing.sql', { content: 'disk', exists: true })
    useWorkspaceStore.getState().updateContent('existing.sql', 'unsaved content')
    vi.mocked(writeFile).mockResolvedValue({ path: 'existing_copy.sql', written: true, etag: '1' })
    renderFiles()
    fireEvent.contextMenu(screen.getByText('existing.sql'))
    await userEvent.click(await screen.findByRole('menuitem', { name: 'Duplicate file…' }))
    expect(await screen.findByLabelText('Duplicate destination')).toHaveValue('existing_copy.sql')
    await userEvent.click(screen.getByRole('button', { name: 'Create' }))
    await waitFor(() => expect(writeFile).toHaveBeenCalledWith('existing_copy.sql', 'unsaved content', 'create-only'))
    expect(fetchFileContentWithMeta).not.toHaveBeenCalled()
    expect(useWorkspaceStore.getState().buffers.find((b) => b.path === 'existing.sql')?.content).toBe('unsaved content')
  })

  it('disables new and delete actions in viewer mode', () => {
    useLayoutStore.getState().setViewerMode(true)
    renderFiles()
    expect(screen.getByRole('button', { name: 'New file' })).toBeDisabled()
    expect(screen.getByRole('button', { name: 'New flowgroup' })).toBeDisabled()
    expect(screen.getByRole('button', { name: 'Delete existing.sql' })).toBeDisabled()
  })
})
