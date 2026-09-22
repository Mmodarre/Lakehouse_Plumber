import { act, renderHook } from '@testing-library/react'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import { previewTemplate, type TemplatePreviewResponse } from '@/api/template-authoring'
import { useWorkspaceStore } from '@/store/workspaceStore'
import { markTemplatePreviewsStale, useTemplatePreviewStore } from '@/store/templatePreviewStore'
import { useTemplatePreview } from '../useTemplatePreview'

vi.mock('@/api/template-authoring', () => ({ previewTemplate: vi.fn() }))
const path = 'templates/test.yaml'
const ready: TemplatePreviewResponse = { request_revision: '1', source_hash: 'hash', stage: 'expanded', status: 'ready', diagnostics: [], missing_parameters: [], expanded_actions: [], saved_dependencies: [] }
beforeEach(() => {
  vi.clearAllMocks()
  useWorkspaceStore.getState().closeAllBuffers()
  useWorkspaceStore.getState().ensureProjectScope('preview-project')
  useWorkspaceStore.getState().openBuffer(path, { content: 'name: test\nactions: []\n', exists: true })
  useWorkspaceStore.getState().openEntityTab('', 'test', path, { docKind: 'template', view: 'preview' })
  useTemplatePreviewStore.setState({ sessions: {}, dependenciesRevision: 0 })
})

describe('unsaved template preview lifecycle', () => {
  it('sends current unsaved source and explicit false/null/empty values without writing a file', async () => {
    vi.mocked(previewTemplate).mockResolvedValue(ready)
    const { result } = renderHook(() => useTemplatePreview(path))
    act(() => { useWorkspaceStore.getState().updateContent(path, 'name: changed\nactions: []\n'); result.current.update({ values: { flag: false, none: null, text: '', items: [] } }) })
    await act(() => result.current.run())
    expect(previewTemplate).toHaveBeenCalledWith(expect.objectContaining({ source_yaml: 'name: changed\nactions: []\n', sample_parameters: { flag: false, none: null, text: '', items: [] } }), expect.any(AbortSignal))
    expect(result.current.session.result).toEqual(ready)
    expect(useWorkspaceStore.getState().buffers[0].isDirty).toBe(true)
  })
  it('does not accept a response for a source that changed during the request', async () => {
    let finish!: (value: TemplatePreviewResponse) => void
    vi.mocked(previewTemplate).mockImplementation(() => new Promise((resolve) => { finish = resolve }))
    const { result } = renderHook(() => useTemplatePreview(path))
    let pending!: Promise<void>
    act(() => { pending = result.current.run() })
    act(() => useWorkspaceStore.getState().updateContent(path, 'name: newer\nactions: []\n'))
    await act(async () => { finish(ready); await pending })
    expect(result.current.session.result).toBeUndefined()
    expect(result.current.error).toMatch(/changed during preview/)
  })
  it('marks a previous result stale after a saved dependency changes, including while away', async () => {
    vi.mocked(previewTemplate).mockResolvedValue(ready)
    const first = renderHook(() => useTemplatePreview(path))
    await act(() => first.result.current.run())
    expect(first.result.current.stale).toBe(false)
    first.unmount()
    act(() => markTemplatePreviewsStale())
    const second = renderHook(() => useTemplatePreview(path))
    expect(second.result.current.stale).toBe(true)
    expect(second.result.current.session.result).toEqual(ready)
  })
  it('retains samples across view switches and clears them when the document closes', () => {
    const first = renderHook(() => useTemplatePreview(path))
    act(() => first.result.current.update({ values: { items: [{ id: 1 }] } }))
    first.unmount()
    const second = renderHook(() => useTemplatePreview(path))
    expect(second.result.current.session.values).toEqual({ items: [{ id: 1 }] })
    second.unmount()
    act(() => useWorkspaceStore.getState().closeAllBuffers())
    expect(useTemplatePreviewStore.getState().sessions).toEqual({})
  })
  it('aborts a request when its view unmounts and does not resurrect closed state', async () => {
    let finish!: (value: TemplatePreviewResponse) => void
    vi.mocked(previewTemplate).mockImplementation(() => new Promise((resolve) => { finish = resolve }))
    const first = renderHook(() => useTemplatePreview(path))
    let pending!: Promise<void>
    act(() => { pending = first.result.current.run() })
    first.unmount()
    expect(vi.mocked(previewTemplate).mock.calls[0][1]?.aborted).toBe(true)
    await act(async () => { finish(ready); await pending })
    expect(useTemplatePreviewStore.getState().sessions).toEqual({})
  })
})
