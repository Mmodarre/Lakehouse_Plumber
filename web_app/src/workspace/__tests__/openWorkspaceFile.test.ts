import { beforeEach, describe, expect, it, vi } from 'vitest'
import { useWorkspaceStore, workspaceTabId } from '@/store/workspaceStore'
import { fetchFileContentWithMeta } from '@/api/files'
import { openWorkspaceFile } from '../openWorkspaceFile'

vi.mock('@/api/files', () => ({ fetchFileContentWithMeta: vi.fn() }))
beforeEach(() => { useWorkspaceStore.getState().closeAllBuffers(); vi.clearAllMocks() })

describe('path-aware source navigation', () => {
  it.each(['entity', 'config'] as const)('focuses an existing %s tab and preserves its dirty content', async (kind) => {
    const ws = useWorkspaceStore.getState()
    ws.openBuffer('shared.yaml', { content: 'disk', exists: true })
    if (kind === 'entity') ws.openEntityTab('p', 'f', 'shared.yaml')
    else ws.openConfigTab('shared.yaml', 'pipeline')
    ws.updateContent('shared.yaml', 'unsaved')
    ws.openProjectMap()
    await openWorkspaceFile('shared.yaml', { source: true, line: 17 })
    const state = useWorkspaceStore.getState()
    expect(state.activePath).toBe(kind === 'entity' ? 'entity:p/f' : 'config:shared.yaml')
    expect(state.tabs[0]).toMatchObject({ view: kind === 'entity' ? 'code' : 'yaml' })
    expect(state.buffers[0].content).toBe('unsaved')
    expect(state.revealLocation).toMatchObject({ path: 'shared.yaml', line: 17 })
    expect(fetchFileContentWithMeta).not.toHaveBeenCalled()
  })

  it('recognized config files open the same structured tab from either entry point', async () => {
    vi.mocked(fetchFileContentWithMeta).mockResolvedValue({ content: 'name: project', etag: '1' })
    await openWorkspaceFile('lhp.yaml')
    useWorkspaceStore.getState().openConfigTab('lhp.yaml', 'project')
    expect(useWorkspaceStore.getState().tabs.map(workspaceTabId)).toEqual(['config:lhp.yaml'])
  })

  it('opens an existing resource source as code instead of leaving a placeholder selected', async () => {
    const ws = useWorkspaceStore.getState()
    ws.openResourceTab('preset', 'shared', 'presets/shared.yaml')
    ws.openBuffer('presets/shared.yaml', { content: 'disk', exists: true })
    await openWorkspaceFile('presets/shared.yaml', { source: true })
    expect(useWorkspaceStore.getState().tabs).toEqual([{ kind: 'file', path: 'presets/shared.yaml' }])
  })
  it('a slow file open does not steal focus from newer navigation', async () => {
    let finish!: (value: { content: string; etag: string }) => void
    vi.mocked(fetchFileContentWithMeta).mockImplementationOnce(() => new Promise((resolve) => { finish = resolve }))
    const pending = openWorkspaceFile('slow.sql', { source: true, line: 17 })
    useWorkspaceStore.getState().openProjectMap()
    finish({ content: 'select 1', etag: '1' })
    await pending
    expect(useWorkspaceStore.getState().activePath).toBe('project-map')
    expect(useWorkspaceStore.getState().buffers.find((b) => b.path === 'slow.sql')?.content).toBe('select 1')
    expect(useWorkspaceStore.getState().revealLocation).toBeNull()
  })

})
