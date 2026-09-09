import { beforeEach, describe, expect, it } from 'vitest'
import { useWorkspaceStore } from '../workspaceStore'
import { useDocumentStore } from '../documentStore'
import { useDocumentHistoryStore } from '../documentHistoryStore'
import { useLayoutStore } from '../layoutStore'

const path = 'pipelines/p.yaml'
const before = 'pipeline: p\nflowgroup: g\nactions: []\n'
const after = 'pipeline: p\nflowgroup: g\ndescription: changed\nactions: []\n'
describe('graph change recovery', () => {
  beforeEach(() => {
    useWorkspaceStore.setState({ tabs: [], buffers: [], activePath: null })
    useLayoutStore.setState({ viewerMode: false })
    useDocumentHistoryStore.setState({ histories: {} })
    useDocumentStore.setState({ docs: {} })
    useWorkspaceStore.getState().openBuffer(path, { content: before, exists: true })
    useDocumentStore.getState().open(path, 'flowgroup')
    useWorkspaceStore.getState().updateContent(path, after)
    useDocumentStore.getState().reparse(path, after)
    useDocumentHistoryStore.getState().record(path, before, after)
  })
  it('undoes and redoes without writing disk and restores dirty status', () => {
    expect(useDocumentHistoryStore.getState().apply(path, 'undo')).toBe(true)
    expect(useWorkspaceStore.getState().buffers[0]).toMatchObject({ content: before, isDirty: false })
    expect(useDocumentHistoryStore.getState().apply(path, 'redo')).toBe(true)
    expect(useWorkspaceStore.getState().buffers[0]).toMatchObject({ content: after, isDirty: true })
  })
  it('never overwrites a newer Code edit', () => {
    useWorkspaceStore.getState().updateContent(path, after + '# editing\n')
    expect(useDocumentHistoryStore.getState().apply(path, 'undo')).toBe(false)
    expect(useWorkspaceStore.getState().buffers[0].content).toContain('# editing')
  })
  it('clears recovery history across served projects', () => {
    useWorkspaceStore.setState({ projectRoot: '/other/project' })
    expect(useDocumentHistoryStore.getState().histories).toEqual({})
  })
  it('refuses mutation and undo in viewer mode', () => {
    useLayoutStore.setState({ viewerMode: true })
    expect(useDocumentHistoryStore.getState().apply(path, 'undo')).toBe(false)
    expect(useDocumentStore.getState().mutate(path, () => { throw new Error('must not execute') })).toBe(false)
  })
})
