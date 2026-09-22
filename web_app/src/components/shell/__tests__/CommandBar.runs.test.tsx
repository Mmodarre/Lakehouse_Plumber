import { fireEvent, render, screen, waitFor } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import { CommandBar } from '../CommandBar'
import { useUIStore } from '../../../store/uiStore'
import { useWorkspaceStore } from '../../../store/workspaceStore'
import { useLayoutStore } from '../../../store/layoutStore'
const mocks = vi.hoisted(() => ({ startValidate: vi.fn(), startGenerate: vi.fn(), save: vi.fn(), preparedRun: vi.fn() }))
vi.mock('../../../hooks/useProject', () => ({ useProject: () => ({ data: { name: 'Example', version: '0.9.2' } }), useHealth: () => ({ data: { root: '/example', status: 'healthy', project_state: 'ok' }, isError: false, isPending: false }) }))
vi.mock('../../../hooks/useEnvironments', () => ({ useEnvironments: () => ({ data: { environments: ['test', 'prod'] } }) }))
vi.mock('../../../hooks/useFiles', () => ({ useFileList: () => ({ data: undefined }) }))
vi.mock('../../../store/runStore', async (original) => ({ ...await original<object>(), startRunWithInputs: mocks.preparedRun, useRunController: () => ({ ...mocks, isRunning: false, abort: vi.fn(), queueValidate: vi.fn() }) }))
vi.mock('../../../workspace/persistBuffer', () => ({ saveAllWorkspaceBuffers: mocks.save }))
vi.mock('../center/PipelineFilter', () => ({ PipelineFilter: () => <span>All pipelines</span> }))
vi.mock('../../sandbox/SandboxControl', () => ({ SandboxControl: () => null }))
vi.mock('../../layout/ThemeToggle', () => ({ ThemeToggle: () => null }))
function mount() {
  const client = new QueryClient()
  return render(<QueryClientProvider client={client}><CommandBar /></QueryClientProvider>)
}
beforeEach(() => {
  vi.clearAllMocks()
  useUIStore.setState({ selectedEnv: '', environmentProject: null, environmentByProject: {}, selectedPipelineConfig: null, sandboxEnabled: false, pipelineFilter: null })
  useWorkspaceStore.setState({ buffers: [], tabs: [], activePath: null })
  useLayoutStore.setState({ viewerMode: false })
  useWorkspaceStore.getState().openBuffer('a.yaml', { content: 'saved', etag: 'old', exists: true })
  useWorkspaceStore.getState().updateContent('a.yaml', 'changed')
})
describe('explicit execution inputs', () => {
  it('chooses an available environment and saves before Generate with auto-validation suppressed', async () => {
    mocks.save.mockResolvedValue(true)
    mount()
    expect(useUIStore.getState().selectedEnv).toBe('test')
    fireEvent.click(screen.getByRole('button', { name: 'Save & Generate' }))
    await waitFor(() => expect(mocks.preparedRun).toHaveBeenCalledWith(expect.objectContaining({ path: '/api/generate/stream', env: 'test' }), expect.anything()))
    expect(mocks.save).toHaveBeenCalledWith(expect.anything(), expect.anything(), { validate: false })
    expect(mocks.startValidate).not.toHaveBeenCalled()
  })
  it('blocks execution if saving fails or newer edits remain', async () => {
    mocks.save.mockResolvedValue(false)
    mount()
    fireEvent.click(screen.getByRole('button', { name: 'Save & Validate' }))
    await waitFor(() => expect(mocks.save).toHaveBeenCalledOnce())
    expect(mocks.startValidate).not.toHaveBeenCalled()
    expect(mocks.preparedRun).not.toHaveBeenCalled()
  })
  it('keeps the clicked environment and pipeline while save is pending', async () => {
    let resolve!: (success: boolean) => void
    mocks.save.mockImplementation(() => new Promise((done) => { resolve = done }))
    mount()
    fireEvent.click(screen.getByRole('button', { name: 'Save & Generate' }))
    useUIStore.setState({ selectedEnv: 'prod', pipelineFilter: 'different', selectedPipelineConfig: 'other.yaml' })
    resolve(true)
    await waitFor(() => expect(mocks.preparedRun).toHaveBeenCalledWith(expect.objectContaining({ env: 'test', pipeline: undefined, pipeline_config: undefined }), expect.anything()))
  })
  it('keeps Generate unavailable in viewer mode', () => {
    useLayoutStore.setState({ viewerMode: true })
    mount()
    expect(screen.getByRole('button', { name: 'Save & Generate' })).toBeDisabled()
  })
})
