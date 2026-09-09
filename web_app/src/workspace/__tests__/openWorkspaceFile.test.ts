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
  it('captures and focuses a malformed structured draft before attempting navigation', async () => {
    const ws = useWorkspaceStore.getState()
    ws.openBuffer('existing.yaml', { content: 'name: existing', exists: true })
    const draft = document.createElement('textarea')
    draft.dataset.workspaceDraft = 'true'
    draft.addEventListener('blur', () => draft.setAttribute('aria-invalid', 'true'))
    document.body.append(draft)
    draft.focus()
    try {
      await openWorkspaceFile('templates/next.yaml')
      expect(document.activeElement).toBe(draft)
      expect(useWorkspaceStore.getState().activePath).toBe('existing.yaml')
      expect(useWorkspaceStore.getState().tabs).toHaveLength(1)
      expect(fetchFileContentWithMeta).not.toHaveBeenCalled()
    } finally { draft.remove() }
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


describe('template authoring navigation', () => {
  it('upgrades a raw dirty template to the builder, preserving its buffer and pin', async () => {
    const ws = useWorkspaceStore.getState()
    const path = 'templates/ingestion/example.yaml'
    ws.openBuffer(path, { content: 'name: original\nactions: []\n', exists: true })
    ws.updateContent(path, 'name: unsaved\nactions: []\n')
    ws.togglePinned(path)
    await openWorkspaceFile(path)
    const state = useWorkspaceStore.getState()
    expect(state.tabs).toHaveLength(1)
    expect(state.tabs[0]).toMatchObject({ kind: 'entity', docKind: 'template', view: 'builder', filePath: path })
    expect(state.buffers[0].content).toContain('unsaved')
    expect(state.buffers[0].isDirty).toBe(true)
    expect(state.pinnedTabIds).toEqual([workspaceTabId(state.tabs[0])])
    expect(fetchFileContentWithMeta).not.toHaveBeenCalled()
  })
  it('opens template source links in Code and allows returning to Preview in the same tab', async () => {
    vi.mocked(fetchFileContentWithMeta).mockResolvedValue({ content: 'name: display\nactions: []\n', etag: '1' })
    await openWorkspaceFile('templates/file.yaml', { source: true, line: 2 })
    const ws = useWorkspaceStore.getState()
    expect(ws.tabs[0]).toMatchObject({ view: 'code', docKind: 'template' })
    ws.setTabView(workspaceTabId(ws.tabs[0]), 'preview')
    await openWorkspaceFile('templates/file.yaml')
    expect(useWorkspaceStore.getState().tabs).toHaveLength(1)
    expect(useWorkspaceStore.getState().tabs[0]).toMatchObject({ view: 'preview' })
  })
  it.each(['templates/job_config.yaml', 'templates/pipeline_config_orders.yaml'])(
    'keeps %s in template authoring despite its configuration-looking name', async (path) => {
      vi.mocked(fetchFileContentWithMeta).mockResolvedValue({ content: 'name: reusable\nactions: []\n', etag: '1' })
      await openWorkspaceFile(path)
      let state = useWorkspaceStore.getState()
      expect(state.tabs).toHaveLength(1)
      expect(state.tabs[0]).toMatchObject({ kind: 'entity', docKind: 'template', view: 'builder', filePath: path })
      state.updateContent(path, 'name: unsaved\nactions: []\n')
      // A generic configuration source link must still respect the document path.
      await openWorkspaceFile(path, { configKind: 'job', source: true, line: 2 })
      state = useWorkspaceStore.getState()
      expect(state.tabs).toHaveLength(1)
      expect(state.tabs[0]).toMatchObject({ kind: 'entity', docKind: 'template', view: 'code', filePath: path })
      expect(state.buffers[0].content).toContain('unsaved')
      expect(state.buffers[0].isDirty).toBe(true)
      expect(state.revealLocation).toMatchObject({ path, line: 2 })
    },
  )
  it('rejects template-only views for a flowgroup', () => {
    const ws = useWorkspaceStore.getState()
    ws.openEntityTab('p', 'f', 'pipelines/f.yaml')
    ws.setTabView('entity:p/f', 'builder')
    expect(useWorkspaceStore.getState().tabs[0]).toMatchObject({ view: 'graph' })
  })
})
