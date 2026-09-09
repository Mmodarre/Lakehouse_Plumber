import { beforeEach, describe, expect, it } from 'vitest'
import { useWorkspaceStore, workspaceTabId } from '@/store/workspaceStore'
import { closingDocumentPaths, tabCloseTargets } from '../tabCommands'

const ws = () => useWorkspaceStore.getState()
beforeEach(() => ws().closeAllBuffers())

describe('safe workspace document ownership', () => {
  it('upgrades raw config tabs without losing edits, position, focus or pins', () => {
    ws().openBuffer('lhp.yaml', { content: 'name: old', exists: true })
    ws().updateContent('lhp.yaml', 'name: new')
    ws().togglePinned('lhp.yaml')
    ws().openConfigTab('lhp.yaml', 'project')
    expect(ws().tabs.map(workspaceTabId)).toEqual(['config:lhp.yaml'])
    expect(ws().buffers[0].content).toBe('name: new')
    expect(ws().pinnedTabIds).toEqual(['config:lhp.yaml'])
    ws().openBuffer('lhp.yaml')
    expect(ws().activePath).toBe('config:lhp.yaml')
  })

  it('retains shared file content until the last entity closes', () => {
    ws().openBuffer('shared.yaml', { content: 'original', exists: true })
    ws().openEntityTab('p', 'first', 'shared.yaml')
    ws().openEntityTab('p', 'second', 'shared.yaml')
    ws().updateContent('shared.yaml', 'unsaved')
    expect(closingDocumentPaths(ws().tabs, ['entity:p/first'])).toEqual([])
    ws().closeTab('entity:p/first')
    expect(ws().buffers[0].content).toBe('unsaved')
    ws().closeTab('entity:p/second')
    expect(ws().buffers).toEqual([])
  })

  it('save acknowledgement advances only the disk baseline and preserves later edits', () => {
    ws().openBuffer('a.sql', { content: 'disk', etag: '1', exists: true })
    ws().updateContent('a.sql', 'submitted')
    ws().setSaving('a.sql', true)
    ws().updateContent('a.sql', 'typed while saving')
    ws().setEtagAndBaseline('a.sql', '2', 'submitted')
    expect(ws().buffers[0]).toMatchObject({ content: 'typed while saving', originalContent: 'submitted', isDirty: true, etag: '2' })
  })
})

describe('tab management commands', () => {
  it('uses the clicked inactive anchor and protects pins in every bulk operation', () => {
    for (const path of ['a.sql', 'b.sql', 'c.sql', 'd.sql']) ws().openBuffer(path, { exists: true })
    ws().setActive('a.sql')
    ws().togglePinned('d.sql')
    ws().updateContent('b.sql', 'edit')
    const targets = (command: Parameters<typeof tabCloseTargets>[4]) => tabCloseTargets(ws().tabs, ws().buffers, ws().pinnedTabIds, 'b.sql', command)
    expect(targets('others')).toEqual(['a.sql', 'c.sql'])
    expect(targets('right')).toEqual(['c.sql'])
    expect(targets('saved')).toEqual(['a.sql', 'c.sql'])
    expect(targets('all')).toEqual(['a.sql', 'b.sql', 'c.sql'])
    expect(targets('close')).toEqual(['b.sql'])
  })

  it('bulk close picks a surviving neighbor and leaves unrelated buffers intact', () => {
    for (const path of ['a.sql', 'b.sql', 'c.sql']) ws().openBuffer(path, { exists: true })
    ws().setActive('b.sql')
    ws().closeTabs(['a.sql', 'b.sql'])
    expect(ws().activePath).toBe('c.sql')
    expect(ws().buffers.map((b) => b.path)).toEqual(['c.sql'])
  })

  it('reopen reloads from disk and cannot resurrect an explicitly discarded draft', () => {
    ws().openBuffer('a.sql', { content: 'disk', exists: true })
    ws().updateContent('a.sql', 'discarded secret edit')
    ws().closeTab('a.sql')
    ws().reopenClosedTab()
    expect(ws().activePath).toBe('a.sql')
    expect(ws().buffers[0]).toMatchObject({ content: '', loading: true, isDirty: false })
  })

  it('reordering changes position without changing active selection or buffers', () => {
    for (const path of ['a.sql', 'b.sql', 'c.sql']) ws().openBuffer(path)
    const buffers = ws().buffers
    ws().moveTab('b.sql', 1)
    expect(ws().tabs.map(workspaceTabId)).toEqual(['a.sql', 'c.sql', 'b.sql'])
    expect(ws().activePath).toBe('c.sql')
    expect(ws().buffers).toBe(buffers)
  })
})
