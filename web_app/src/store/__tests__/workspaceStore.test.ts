import { describe, expect, it, vi } from 'vitest'

// workspaceStore persists to localStorage and runs a hydration fixup at
// module load, so every scenario imports a fresh copy of the module via
// vi.resetModules() + dynamic import (same pattern as themeStore.test.ts).

async function loadStore(opts: { keepStorage?: boolean } = {}) {
  vi.resetModules()
  if (!opts.keepStorage) localStorage.clear()
  const mod = await import('@/store/workspaceStore')
  return mod
}

describe('workspaceStore', () => {
  it('openBuffer creates a buffer, derives language/category, and activates it', async () => {
    const { useWorkspaceStore } = await loadStore()
    useWorkspaceStore.getState().openBuffer('pipelines/raw/orders.yaml', {
      content: 'pipeline: raw\n',
      etag: 'e1',
      exists: true,
    })
    const s = useWorkspaceStore.getState()
    expect(s.buffers).toHaveLength(1)
    expect(s.activePath).toBe('pipelines/raw/orders.yaml')
    const buf = s.buffers[0]
    expect(buf.language).toBe('yaml')
    expect(buf.category).toBe('yaml')
    expect(buf.content).toBe('pipeline: raw\n')
    expect(buf.originalContent).toBe('pipeline: raw\n')
    expect(buf.etag).toBe('e1')
    expect(buf.isDirty).toBe(false)
    expect(buf.exists).toBe(true)
  })

  it('openBuffer is a no-op for an already-open path (activates, never clobbers)', async () => {
    const { useWorkspaceStore } = await loadStore()
    const store = useWorkspaceStore.getState()
    store.openBuffer('a.sql', { content: 'select 1', etag: 'e1', exists: true })
    store.openBuffer('b.sql', { content: 'select 2', etag: 'e2', exists: true })
    useWorkspaceStore.getState().updateContent('a.sql', 'select 1 -- edited')

    // Re-open a.sql with a different seed: must only focus it.
    useWorkspaceStore.getState().openBuffer('a.sql', { content: 'CLOBBER', etag: 'e9' })
    const s = useWorkspaceStore.getState()
    expect(s.activePath).toBe('a.sql')
    const a = s.buffers.find((b) => b.path === 'a.sql')
    expect(a?.content).toBe('select 1 -- edited')
    expect(a?.etag).toBe('e1')
    expect(a?.isDirty).toBe(true)
  })

  it('openBuffer with activate:false keeps the current active buffer', async () => {
    const { useWorkspaceStore } = await loadStore()
    useWorkspaceStore.getState().openBuffer('a.yaml', { content: '' })
    useWorkspaceStore.getState().openBuffer('b.yaml', { content: '', activate: false })
    expect(useWorkspaceStore.getState().activePath).toBe('a.yaml')
    expect(useWorkspaceStore.getState().buffers).toHaveLength(2)
  })

  it('updateContent recomputes isDirty against originalContent (both directions)', async () => {
    const { useWorkspaceStore } = await loadStore()
    useWorkspaceStore.getState().openBuffer('a.py', { content: 'x = 1\n', exists: true })
    useWorkspaceStore.getState().updateContent('a.py', 'x = 2\n')
    expect(useWorkspaceStore.getState().buffers[0].isDirty).toBe(true)
    // Typing back to the original text clears the dirty flag.
    useWorkspaceStore.getState().updateContent('a.py', 'x = 1\n')
    expect(useWorkspaceStore.getState().buffers[0].isDirty).toBe(false)
  })

  it('updateContent and setDirty do not churn state when nothing changes (P3 regression)', async () => {
    const { useWorkspaceStore } = await loadStore()
    useWorkspaceStore.getState().openBuffer('a.py', { content: 'x = 1\n', exists: true })
    useWorkspaceStore.getState().updateContent('a.py', 'x = 2\n')
    const before = useWorkspaceStore.getState().buffers

    // Same content again → identical array identity (no re-render fanout).
    useWorkspaceStore.getState().updateContent('a.py', 'x = 2\n')
    expect(useWorkspaceStore.getState().buffers).toBe(before)

    // Per-keystroke dirty flag is idempotent once already dirty.
    useWorkspaceStore.getState().setDirty('a.py', true)
    expect(useWorkspaceStore.getState().buffers).toBe(before)

    // Unknown path → no churn either.
    useWorkspaceStore.getState().setDirty('nope.py', true)
    expect(useWorkspaceStore.getState().buffers).toBe(before)
  })

  it('setEtagAndBaseline moves the baseline and clears dirty/new flags', async () => {
    const { useWorkspaceStore } = await loadStore()
    useWorkspaceStore.getState().openBuffer('new.yaml', {
      content: 'pipeline: p\n',
      originalContent: '',
      isDirty: true,
      isNew: true,
      exists: false,
    })
    useWorkspaceStore.getState().setEtagAndBaseline('new.yaml', 'e2', 'pipeline: p\n')
    const buf = useWorkspaceStore.getState().buffers[0]
    expect(buf.etag).toBe('e2')
    expect(buf.originalContent).toBe('pipeline: p\n')
    expect(buf.isDirty).toBe(false)
    expect(buf.isNew).toBe(false)
    expect(buf.exists).toBe(true)
  })

  it('replaceBuffer swaps in disk content and resets the dirty flag (412 reload)', async () => {
    const { useWorkspaceStore } = await loadStore()
    useWorkspaceStore.getState().openBuffer('a.yaml', { content: 'v1', etag: 'e1', exists: true })
    useWorkspaceStore.getState().updateContent('a.yaml', 'mine')
    useWorkspaceStore.getState().replaceBuffer('a.yaml', 'theirs', 'e2')
    const buf = useWorkspaceStore.getState().buffers[0]
    expect(buf.content).toBe('theirs')
    expect(buf.originalContent).toBe('theirs')
    expect(buf.etag).toBe('e2')
    expect(buf.isDirty).toBe(false)
  })

  it('closeBuffer focuses a neighbour when the active tab closes', async () => {
    const { useWorkspaceStore } = await loadStore()
    const open = (p: string) => useWorkspaceStore.getState().openBuffer(p, { content: '' })
    open('a.yaml')
    open('b.yaml')
    open('c.yaml')
    useWorkspaceStore.getState().setActive('b.yaml')
    useWorkspaceStore.getState().closeBuffer('b.yaml')
    // The next tab slides into the closed slot.
    expect(useWorkspaceStore.getState().activePath).toBe('c.yaml')
    // Closing a background tab keeps the active one.
    useWorkspaceStore.getState().closeBuffer('a.yaml')
    expect(useWorkspaceStore.getState().activePath).toBe('c.yaml')
    // Closing the last buffer clears the focus.
    useWorkspaceStore.getState().closeBuffer('c.yaml')
    expect(useWorkspaceStore.getState().activePath).toBeNull()
    expect(useWorkspaceStore.getState().buffers).toHaveLength(0)
  })

  it('closeAllBuffers clears every buffer and the active path', async () => {
    const { useWorkspaceStore } = await loadStore()
    useWorkspaceStore.getState().openBuffer('a.yaml', { content: '' })
    useWorkspaceStore.getState().openBuffer('b.yaml', { content: '' })
    useWorkspaceStore.getState().closeAllBuffers()
    expect(useWorkspaceStore.getState().buffers).toHaveLength(0)
    expect(useWorkspaceStore.getState().activePath).toBeNull()
  })

  it('closeBuffer on an unknown path is a no-op', async () => {
    const { useWorkspaceStore } = await loadStore()
    useWorkspaceStore.getState().openBuffer('a.yaml', { content: '' })
    const before = useWorkspaceStore.getState().buffers
    useWorkspaceStore.getState().closeBuffer('ghost.yaml')
    expect(useWorkspaceStore.getState().buffers).toBe(before)
    expect(useWorkspaceStore.getState().activePath).toBe('a.yaml')
  })

  it('setActive only accepts open buffers (or null), and never churns', async () => {
    const { useWorkspaceStore } = await loadStore()
    useWorkspaceStore.getState().openBuffer('a.yaml', { content: '' })
    useWorkspaceStore.getState().openBuffer('b.yaml', { content: '' })

    // Unknown path → ignored.
    useWorkspaceStore.getState().setActive('ghost.yaml')
    expect(useWorkspaceStore.getState().activePath).toBe('b.yaml')

    // Same path → no-op (already active).
    useWorkspaceStore.getState().setActive('b.yaml')
    expect(useWorkspaceStore.getState().activePath).toBe('b.yaml')

    // Open buffer → focused; null → back to page view.
    useWorkspaceStore.getState().setActive('a.yaml')
    expect(useWorkspaceStore.getState().activePath).toBe('a.yaml')
    useWorkspaceStore.getState().setActive(null)
    expect(useWorkspaceStore.getState().activePath).toBeNull()
  })

  it('replaceBuffer heals a load-failed buffer (loadFailed/loading cleared)', async () => {
    const { useWorkspaceStore } = await loadStore()
    useWorkspaceStore.getState().openBuffer('a.yaml', { exists: true, loading: true })
    useWorkspaceStore.getState().markLoadFailed('a.yaml')
    expect(useWorkspaceStore.getState().buffers[0].loadFailed).toBe(true)

    useWorkspaceStore.getState().replaceBuffer('a.yaml', 'from disk', 'e3')
    const buf = useWorkspaceStore.getState().buffers[0]
    expect(buf.loadFailed).toBe(false)
    expect(buf.loading).toBe(false)
    expect(buf.content).toBe('from disk')
    expect(buf.etag).toBe('e3')
    expect(buf.isDirty).toBe(false)
    expect(buf.exists).toBe(true)
  })

  it('discardDirty is a no-op when nothing is dirty', async () => {
    const { useWorkspaceStore } = await loadStore()
    useWorkspaceStore.getState().openBuffer('a.yaml', { content: 'clean', exists: true })
    const before = useWorkspaceStore.getState().buffers
    useWorkspaceStore.getState().discardDirty()
    expect(useWorkspaceStore.getState().buffers).toBe(before)
  })

  it('ackRestore clears the restored-dirty prompt counter', async () => {
    const first = await loadStore()
    first.useWorkspaceStore.getState().openBuffer('a.yaml', { content: 'orig', exists: true })
    first.useWorkspaceStore.getState().updateContent('a.yaml', 'edited')

    const second = await loadStore({ keepStorage: true })
    expect(second.useWorkspaceStore.getState().restoredDirtyCount).toBe(1)
    second.useWorkspaceStore.getState().ackRestore()
    expect(second.useWorkspaceStore.getState().restoredDirtyCount).toBe(0)
    // The buffers themselves are untouched — only the prompt flag clears.
    expect(second.useWorkspaceStore.getState().buffers).toHaveLength(1)
  })

  it('discardDirty reverts existing files and drops never-saved new ones', async () => {
    const { useWorkspaceStore } = await loadStore()
    useWorkspaceStore.getState().openBuffer('a.yaml', { content: 'orig', etag: 'e1', exists: true })
    useWorkspaceStore.getState().updateContent('a.yaml', 'edited')
    useWorkspaceStore.getState().openBuffer('scaffold.yaml', {
      content: 'pipeline: p\n',
      originalContent: '',
      isDirty: true,
      isNew: true,
      exists: false,
    })
    useWorkspaceStore.getState().discardDirty()
    const s = useWorkspaceStore.getState()
    expect(s.buffers.map((b) => b.path)).toEqual(['a.yaml'])
    expect(s.buffers[0].content).toBe('orig')
    expect(s.buffers[0].isDirty).toBe(false)
    // The dropped scaffold was active → focus falls back to page view.
    expect(s.activePath).toBeNull()
  })

  it('ensureProjectScope drops persisted buffers from a different project', async () => {
    const { useWorkspaceStore } = await loadStore()
    useWorkspaceStore.getState().ensureProjectScope('/proj/a')
    useWorkspaceStore.getState().openBuffer('a.yaml', { content: '' })
    // Same root → untouched.
    useWorkspaceStore.getState().ensureProjectScope('/proj/a')
    expect(useWorkspaceStore.getState().buffers).toHaveLength(1)
    // Different root → workspace cleared.
    useWorkspaceStore.getState().ensureProjectScope('/proj/b')
    const s = useWorkspaceStore.getState()
    expect(s.buffers).toHaveLength(0)
    expect(s.activePath).toBeNull()
    expect(s.projectRoot).toBe('/proj/b')
  })

  it('persists buffers to localStorage and restores them on a fresh boot', async () => {
    const first = await loadStore()
    first.useWorkspaceStore.getState().openBuffer('a.yaml', {
      content: 'orig',
      etag: 'e1',
      exists: true,
    })
    first.useWorkspaceStore.getState().updateContent('a.yaml', 'unsaved edit')
    first.useWorkspaceStore.getState().setSaving('a.yaml', true)

    // Fresh module load = new browser session hydrating from localStorage.
    const second = await loadStore({ keepStorage: true })
    const s = second.useWorkspaceStore.getState()
    expect(s.buffers).toHaveLength(1)
    expect(s.buffers[0].content).toBe('unsaved edit')
    expect(s.buffers[0].originalContent).toBe('orig')
    expect(s.buffers[0].etag).toBe('e1')
    expect(s.buffers[0].isDirty).toBe(true)
    // Transient flags never survive a restore.
    expect(s.buffers[0].isSaving).toBe(false)
    expect(s.buffers[0].loading).toBe(false)
    expect(s.activePath).toBe('a.yaml')
    // One restored dirty buffer → the one-time restore prompt fires.
    expect(s.restoredDirtyCount).toBe(1)
  })

  it('markLoadFailed blocks the buffer and a retried markLoaded heals it', async () => {
    const { useWorkspaceStore } = await loadStore()
    useWorkspaceStore.getState().openBuffer('sql/a.sql', { exists: true, loading: true })
    useWorkspaceStore.getState().markLoadFailed('sql/a.sql')
    let buf = useWorkspaceStore.getState().buffers[0]
    expect(buf.loadFailed).toBe(true)
    expect(buf.loading).toBe(false)

    // Retry path: markLoaded is accepted on a load-failed buffer (it holds
    // no user edits, so filling it never clobbers anything).
    useWorkspaceStore.getState().markLoaded('sql/a.sql', 'select 1', 'e1')
    buf = useWorkspaceStore.getState().buffers[0]
    expect(buf.loadFailed).toBe(false)
    expect(buf.content).toBe('select 1')
    expect(buf.originalContent).toBe('select 1')
    expect(buf.etag).toBe('e1')
    expect(buf.isDirty).toBe(false)

    // Once loaded, further markLoaded calls no-op (never clobber edits).
    useWorkspaceStore.getState().markLoaded('sql/a.sql', 'CLOBBER', 'e9')
    expect(useWorkspaceStore.getState().buffers[0].content).toBe('select 1')
  })

  it('a storage quota error never throws out of a store action (Fix 4)', async () => {
    const { useWorkspaceStore } = await loadStore()
    const spy = vi.spyOn(Storage.prototype, 'setItem').mockImplementation(() => {
      throw new DOMException('quota exceeded', 'QuotaExceededError')
    })
    try {
      // zustand persist writes synchronously inside set(); both actions
      // must survive the throwing storage.
      expect(() =>
        useWorkspaceStore.getState().openBuffer('big.py', { content: 'x', exists: true }),
      ).not.toThrow()
      expect(() => useWorkspaceStore.getState().updateContent('big.py', 'y')).not.toThrow()
      expect(useWorkspaceStore.getState().buffers[0].content).toBe('y')
    } finally {
      spy.mockRestore()
    }
  })

  it('persists content only for dirty buffers; clean existing buffers restore as loading placeholders', async () => {
    const first = await loadStore()
    first.useWorkspaceStore
      .getState()
      .openBuffer('clean.py', { content: 'clean text', etag: 'e1', exists: true })
    first.useWorkspaceStore
      .getState()
      .openBuffer('dirty.py', { content: 'orig', etag: 'e2', exists: true })
    first.useWorkspaceStore.getState().updateContent('dirty.py', 'edited')

    // The persisted payload never carries the clean buffer's content (quota
    // pressure + stale-clean restores).
    expect(localStorage.getItem('lhp-workspace')).not.toContain('clean text')

    const second = await loadStore({ keepStorage: true })
    const s = second.useWorkspaceStore.getState()
    const clean = s.buffers.find((b) => b.path === 'clean.py')!
    expect(clean.loading).toBe(true)
    expect(clean.content).toBe('')
    expect(clean.isDirty).toBe(false)
    const dirty = s.buffers.find((b) => b.path === 'dirty.py')!
    expect(dirty.loading).toBe(false)
    expect(dirty.content).toBe('edited')
    expect(dirty.originalContent).toBe('orig')
    expect(dirty.isDirty).toBe(true)
    // Only the dirty buffer feeds the restore prompt.
    expect(s.restoredDirtyCount).toBe(1)
  })

  it('isReadOnlyPath covers every backend-403 prefix', async () => {
    const { isReadOnlyPath } = await loadStore()
    expect(isReadOnlyPath('generated/raw/orders.py')).toBe(true)
    expect(isReadOnlyPath('.git/HEAD')).toBe(true)
    expect(isReadOnlyPath('.lhp/logs/run.log')).toBe(true)
    expect(isReadOnlyPath('.lhp/dependencies/graph.json')).toBe(true)
    expect(isReadOnlyPath('.lhp_state.json')).toBe(true)
    expect(isReadOnlyPath('pipelines/raw/orders.yaml')).toBe(false)
  })

  it('drops a persisted activePath that no longer resolves to a buffer', async () => {
    localStorage.clear()
    localStorage.setItem(
      'lhp-workspace',
      JSON.stringify({
        state: { buffers: [], activePath: 'ghost.yaml', projectRoot: '/p' },
        version: 0,
      }),
    )
    const { useWorkspaceStore } = await loadStore({ keepStorage: true })
    expect(useWorkspaceStore.getState().activePath).toBeNull()
    expect(useWorkspaceStore.getState().restoredDirtyCount).toBe(0)
  })
})

describe('workspaceStore tab union (designer tabs)', () => {
  it('openDesignerTab opens an identity-only tab and activates it', async () => {
    const { useWorkspaceStore, designerTabId } = await loadStore()
    useWorkspaceStore.getState().openDesignerTab('bronze', 'orders', 'pipelines/bronze/orders.yaml')
    const s = useWorkspaceStore.getState()
    expect(s.tabs).toEqual([
      {
        kind: 'designer',
        id: 'designer:bronze/orders',
        pipeline: 'bronze',
        flowgroup: 'orders',
        filePath: 'pipelines/bronze/orders.yaml',
      },
    ])
    expect(designerTabId('bronze', 'orders')).toBe('designer:bronze/orders')
    expect(s.activePath).toBe('designer:bronze/orders')
    // No buffer materialises for a designer tab.
    expect(s.buffers).toHaveLength(0)
  })

  it('openDesignerTab is idempotent: re-opening focuses, never duplicates or churns', async () => {
    const { useWorkspaceStore } = await loadStore()
    useWorkspaceStore.getState().openDesignerTab('p', 'f', 'pipelines/p/f.yaml')
    useWorkspaceStore.getState().openBuffer('a.yaml', { content: '' })
    expect(useWorkspaceStore.getState().activePath).toBe('a.yaml')

    // Re-open: focuses the existing tab, keeps its identity and position.
    useWorkspaceStore.getState().openDesignerTab('p', 'f', 'pipelines/p/f.yaml')
    let s = useWorkspaceStore.getState()
    expect(s.activePath).toBe('designer:p/f')
    expect(s.tabs).toHaveLength(2)
    expect(s.tabs[0]).toMatchObject({ kind: 'designer', id: 'designer:p/f' })

    // Re-open while already active → no state churn at all.
    const before = s.tabs
    useWorkspaceStore.getState().openDesignerTab('p', 'f', 'pipelines/p/f.yaml')
    s = useWorkspaceStore.getState()
    expect(s.tabs).toBe(before)
  })

  it('re-opening with a different filePath refreshes the same tab in place (moved file)', async () => {
    const { useWorkspaceStore } = await loadStore()
    useWorkspaceStore.getState().openDesignerTab('p', 'f', 'pipelines/p/f.yaml')
    useWorkspaceStore.getState().openBuffer('a.yaml', { content: '' })

    // Background designer tab: focus + filePath refresh, no duplicate.
    useWorkspaceStore.getState().openDesignerTab('p', 'f', 'pipelines/p/f_moved.yaml')
    let s = useWorkspaceStore.getState()
    expect(s.activePath).toBe('designer:p/f')
    expect(s.tabs).toHaveLength(2)
    expect(s.tabs[0]).toEqual({
      kind: 'designer',
      id: 'designer:p/f',
      pipeline: 'p',
      flowgroup: 'f',
      filePath: 'pipelines/p/f_moved.yaml',
    })

    // Already-active tab: filePath still refreshes in place.
    useWorkspaceStore.getState().openDesignerTab('p', 'f', 'pipelines/p/f_moved_again.yaml')
    s = useWorkspaceStore.getState()
    expect(s.activePath).toBe('designer:p/f')
    expect(s.tabs).toHaveLength(2)
    expect(s.tabs[0]).toMatchObject({ filePath: 'pipelines/p/f_moved_again.yaml' })

    // Unchanged path stays churn-free.
    const before = s.tabs
    useWorkspaceStore.getState().openDesignerTab('p', 'f', 'pipelines/p/f_moved_again.yaml')
    expect(useWorkspaceStore.getState().tabs).toBe(before)
  })

  it('tabs interleave kinds in open order (file tabs are refs to buffers)', async () => {
    const { useWorkspaceStore, workspaceTabId } = await loadStore()
    useWorkspaceStore.getState().openBuffer('a.yaml', { content: '' })
    useWorkspaceStore.getState().openDesignerTab('p', 'f', 'pipelines/p/f.yaml')
    useWorkspaceStore.getState().openBuffer('b.yaml', { content: '' })
    const s = useWorkspaceStore.getState()
    expect(s.tabs.map(workspaceTabId)).toEqual(['a.yaml', 'designer:p/f', 'b.yaml'])
    expect(s.buffers.map((b) => b.path)).toEqual(['a.yaml', 'b.yaml'])
  })

  it('closing the active tab focuses the neighbour regardless of kind', async () => {
    const { useWorkspaceStore } = await loadStore()
    useWorkspaceStore.getState().openBuffer('a.yaml', { content: '' })
    useWorkspaceStore.getState().openDesignerTab('p', 'f', 'pipelines/p/f.yaml')
    useWorkspaceStore.getState().openBuffer('b.yaml', { content: '' })

    // Active file tab closes → the designer neighbour slides into focus.
    useWorkspaceStore.getState().setActive('a.yaml')
    useWorkspaceStore.getState().closeBuffer('a.yaml')
    expect(useWorkspaceStore.getState().activePath).toBe('designer:p/f')

    // Active designer tab closes → the file neighbour takes focus.
    useWorkspaceStore.getState().closeDesignerTab('designer:p/f')
    expect(useWorkspaceStore.getState().activePath).toBe('b.yaml')

    // Last tab closes → back to page view.
    useWorkspaceStore.getState().closeBuffer('b.yaml')
    expect(useWorkspaceStore.getState().activePath).toBeNull()
    expect(useWorkspaceStore.getState().tabs).toHaveLength(0)
  })

  it('closeDesignerTab ignores unknown ids and file paths', async () => {
    const { useWorkspaceStore } = await loadStore()
    useWorkspaceStore.getState().openBuffer('a.yaml', { content: '' })
    useWorkspaceStore.getState().openDesignerTab('p', 'f', 'pipelines/p/f.yaml')
    const before = useWorkspaceStore.getState().tabs
    useWorkspaceStore.getState().closeDesignerTab('designer:ghost/ghost')
    // A file path is not a designer id — never closes the file tab.
    useWorkspaceStore.getState().closeDesignerTab('a.yaml')
    expect(useWorkspaceStore.getState().tabs).toBe(before)
  })

  it('setActive accepts a designer tab id and still rejects unknown ids', async () => {
    const { useWorkspaceStore } = await loadStore()
    useWorkspaceStore.getState().openDesignerTab('p', 'f', 'pipelines/p/f.yaml')
    useWorkspaceStore.getState().openBuffer('a.yaml', { content: '' })
    useWorkspaceStore.getState().setActive('designer:p/f')
    expect(useWorkspaceStore.getState().activePath).toBe('designer:p/f')
    useWorkspaceStore.getState().setActive('designer:ghost/ghost')
    expect(useWorkspaceStore.getState().activePath).toBe('designer:p/f')
  })

  it('closeAllBuffers and ensureProjectScope clear designer tabs too', async () => {
    const { useWorkspaceStore } = await loadStore()
    useWorkspaceStore.getState().ensureProjectScope('/proj/a')
    useWorkspaceStore.getState().openBuffer('a.yaml', { content: '' })
    useWorkspaceStore.getState().openDesignerTab('p', 'f', 'pipelines/p/f.yaml')
    useWorkspaceStore.getState().closeAllBuffers()
    expect(useWorkspaceStore.getState().tabs).toHaveLength(0)
    expect(useWorkspaceStore.getState().activePath).toBeNull()

    useWorkspaceStore.getState().openDesignerTab('p', 'f', 'pipelines/p/f.yaml')
    useWorkspaceStore.getState().ensureProjectScope('/proj/b')
    expect(useWorkspaceStore.getState().tabs).toHaveLength(0)
  })

  it('discardDirty keeps a focused designer tab and drops dropped scaffolds’ tab refs', async () => {
    const { useWorkspaceStore, workspaceTabId } = await loadStore()
    useWorkspaceStore.getState().openBuffer('a.yaml', { content: 'orig', exists: true })
    useWorkspaceStore.getState().updateContent('a.yaml', 'edited')
    useWorkspaceStore.getState().openBuffer('scaffold.yaml', {
      content: 'pipeline: p\n',
      originalContent: '',
      isDirty: true,
      isNew: true,
      exists: false,
    })
    useWorkspaceStore.getState().openDesignerTab('p', 'f', 'pipelines/p/f.yaml')
    useWorkspaceStore.getState().discardDirty()
    const s = useWorkspaceStore.getState()
    // The never-saved scaffold's tab ref is gone with its buffer.
    expect(s.tabs.map(workspaceTabId)).toEqual(['a.yaml', 'designer:p/f'])
    // Designer tabs are never dirty — the active one stays focused.
    expect(s.activePath).toBe('designer:p/f')
  })

  it('designer tabs persist identity and restore on a fresh boot', async () => {
    const first = await loadStore()
    first.useWorkspaceStore.getState().openBuffer('a.yaml', {
      content: 'orig',
      etag: 'e1',
      exists: true,
    })
    first.useWorkspaceStore.getState().openDesignerTab('p', 'f', 'pipelines/p/f.yaml')

    const second = await loadStore({ keepStorage: true })
    const s = second.useWorkspaceStore.getState()
    expect(s.tabs).toHaveLength(2)
    expect(s.tabs[1]).toEqual({
      kind: 'designer',
      id: 'designer:p/f',
      pipeline: 'p',
      flowgroup: 'f',
      filePath: 'pipelines/p/f.yaml',
    })
    // The active designer tab survives the reload.
    expect(s.activePath).toBe('designer:p/f')
  })

  it('rebuilds file tab refs for payloads persisted before the tab union existed', async () => {
    localStorage.clear()
    const buffer = {
      path: 'a.yaml',
      language: 'yaml',
      category: 'yaml',
      content: '',
      originalContent: '',
      isDirty: false,
      isSaving: false,
      etag: 'e1',
      exists: true,
      isNew: false,
      loading: true,
      loadFailed: false,
    }
    localStorage.setItem(
      'lhp-workspace',
      JSON.stringify({
        state: { buffers: [buffer], activePath: 'a.yaml', projectRoot: '/p' },
        version: 0,
      }),
    )
    const { useWorkspaceStore } = await loadStore({ keepStorage: true })
    const s = useWorkspaceStore.getState()
    expect(s.tabs).toEqual([{ kind: 'file', path: 'a.yaml' }])
    expect(s.activePath).toBe('a.yaml')
  })

  it('drops stray file tab refs and a stale active designer id on boot', async () => {
    localStorage.clear()
    localStorage.setItem(
      'lhp-workspace',
      JSON.stringify({
        state: {
          buffers: [],
          tabs: [{ kind: 'file', path: 'ghost.yaml' }],
          activePath: 'designer:p/f',
          projectRoot: '/p',
        },
        version: 0,
      }),
    )
    const { useWorkspaceStore } = await loadStore({ keepStorage: true })
    const s = useWorkspaceStore.getState()
    expect(s.tabs).toHaveLength(0)
    expect(s.activePath).toBeNull()
  })
})

describe('workspaceStore — persist migrate (v1 DesignerTab→EntityTab; v2 Graph|Code)', () => {
  it('converts a persisted flowgroup DesignerTab to EntityTab{view:graph} and remaps activePath', async () => {
    localStorage.clear()
    localStorage.setItem(
      'lhp-workspace',
      JSON.stringify({
        state: {
          buffers: [],
          tabs: [
            {
              kind: 'designer',
              id: 'designer:bronze/orders',
              pipeline: 'bronze',
              flowgroup: 'orders',
              filePath: 'pipelines/bronze/orders.yaml',
            },
          ],
          activePath: 'designer:bronze/orders',
          projectRoot: '/p',
        },
        version: 0,
      }),
    )
    const { useWorkspaceStore } = await loadStore({ keepStorage: true })
    const s = useWorkspaceStore.getState()
    expect(s.tabs).toEqual([
      {
        kind: 'entity',
        pipeline: 'bronze',
        flowgroup: 'orders',
        filePath: 'pipelines/bronze/orders.yaml',
        docKind: 'flowgroup',
        view: 'graph',
      },
    ])
    // The old designer id no longer resolves — activePath follows the entity id.
    expect(s.activePath).toBe('entity:bronze/orders')
  })

  it('converts a persisted template DesignerTab (docKind carried, keyed by filePath)', async () => {
    localStorage.clear()
    localStorage.setItem(
      'lhp-workspace',
      JSON.stringify({
        state: {
          buffers: [],
          tabs: [
            {
              kind: 'designer',
              id: 'designer:tpl:templates/csv.yaml',
              pipeline: '',
              flowgroup: 'csv_tpl',
              filePath: 'templates/csv.yaml',
              docKind: 'template',
            },
          ],
          activePath: 'designer:tpl:templates/csv.yaml',
          projectRoot: '/p',
        },
        version: 0,
      }),
    )
    const { useWorkspaceStore } = await loadStore({ keepStorage: true })
    const s = useWorkspaceStore.getState()
    expect(s.tabs).toEqual([
      {
        kind: 'entity',
        pipeline: '',
        flowgroup: 'csv_tpl',
        filePath: 'templates/csv.yaml',
        docKind: 'template',
        view: 'graph',
      },
    ])
    expect(s.activePath).toBe('entity:tpl:templates/csv.yaml')
  })

  it('leaves file tabs untouched during migration', async () => {
    localStorage.clear()
    localStorage.setItem(
      'lhp-workspace',
      JSON.stringify({
        state: {
          buffers: [
            {
              path: 'a.yaml',
              language: 'yaml',
              category: 'yaml',
              content: '',
              originalContent: '',
              isDirty: false,
              isSaving: false,
              etag: 'e1',
              exists: true,
              isNew: false,
              loading: true,
              loadFailed: false,
            },
          ],
          tabs: [{ kind: 'file', path: 'a.yaml' }],
          activePath: 'a.yaml',
          projectRoot: '/p',
        },
        version: 0,
      }),
    )
    const { useWorkspaceStore } = await loadStore({ keepStorage: true })
    const s = useWorkspaceStore.getState()
    expect(s.tabs).toEqual([{ kind: 'file', path: 'a.yaml' }])
    expect(s.activePath).toBe('a.yaml')
  })

  it('v1 → v2: remaps EntityTab.view form/graph→graph and yaml→code; leaves config tabs', async () => {
    localStorage.clear()
    localStorage.setItem(
      'lhp-workspace',
      JSON.stringify({
        state: {
          buffers: [],
          tabs: [
            {
              kind: 'entity',
              pipeline: 'p',
              flowgroup: 'a',
              filePath: 'pipelines/p/a.yaml',
              docKind: 'flowgroup',
              view: 'form',
            },
            {
              kind: 'entity',
              pipeline: 'p',
              flowgroup: 'b',
              filePath: 'pipelines/p/b.yaml',
              docKind: 'flowgroup',
              view: 'yaml',
            },
            {
              kind: 'entity',
              pipeline: 'p',
              flowgroup: 'c',
              filePath: 'pipelines/p/c.yaml',
              docKind: 'flowgroup',
              view: 'graph',
            },
            {
              kind: 'config',
              path: 'config/pipeline_config_dev.yaml',
              configKind: 'pipeline',
              view: 'yaml',
            },
          ],
          activePath: 'entity:p/a',
          projectRoot: '/p',
        },
        version: 1,
      }),
    )
    const { useWorkspaceStore } = await loadStore({ keepStorage: true })
    const s = useWorkspaceStore.getState()
    // Form/Graph → Graph, YAML → Code (the yaml view folds into Code); the
    // config tab's Form|YAML view is untouched.
    expect(s.tabs).toEqual([
      {
        kind: 'entity',
        pipeline: 'p',
        flowgroup: 'a',
        filePath: 'pipelines/p/a.yaml',
        docKind: 'flowgroup',
        view: 'graph',
      },
      {
        kind: 'entity',
        pipeline: 'p',
        flowgroup: 'b',
        filePath: 'pipelines/p/b.yaml',
        docKind: 'flowgroup',
        view: 'code',
      },
      {
        kind: 'entity',
        pipeline: 'p',
        flowgroup: 'c',
        filePath: 'pipelines/p/c.yaml',
        docKind: 'flowgroup',
        view: 'graph',
      },
      {
        kind: 'config',
        path: 'config/pipeline_config_dev.yaml',
        configKind: 'pipeline',
        view: 'yaml',
      },
    ])
    expect(s.activePath).toBe('entity:p/a')
  })
})

describe('workspaceStore — center tab strip (entity/config/map/table/resource)', () => {
  it('openEntityTab opens a typed entity tab keyed by pipeline/flowgroup and focuses it', async () => {
    const { useWorkspaceStore, entityTabId } = await loadStore()
    useWorkspaceStore.getState().openEntityTab('bronze', 'orders', 'pipelines/bronze/orders.yaml')
    const s = useWorkspaceStore.getState()
    expect(s.tabs).toEqual([
      {
        kind: 'entity',
        pipeline: 'bronze',
        flowgroup: 'orders',
        filePath: 'pipelines/bronze/orders.yaml',
        docKind: 'flowgroup',
        view: 'graph',
      },
    ])
    expect(s.activePath).toBe(entityTabId('bronze', 'orders'))
    // No buffer materialises for an entity tab opened cold.
    expect(s.buffers).toHaveLength(0)
  })

  it('openEntityTab is idempotent-focus: re-opening never duplicates or churns', async () => {
    const { useWorkspaceStore } = await loadStore()
    useWorkspaceStore.getState().openEntityTab('p', 'f', 'pipelines/p/f.yaml')
    useWorkspaceStore.getState().openBuffer('a.yaml', { content: '' })
    expect(useWorkspaceStore.getState().activePath).toBe('a.yaml')

    // Re-open focuses the existing tab, one tab only.
    useWorkspaceStore.getState().openEntityTab('p', 'f', 'pipelines/p/f.yaml')
    let s = useWorkspaceStore.getState()
    expect(s.activePath).toBe('entity:p/f')
    expect(s.tabs).toHaveLength(2)

    // Re-open while already active + unchanged → zero churn.
    const before = s.tabs
    useWorkspaceStore.getState().openEntityTab('p', 'f', 'pipelines/p/f.yaml')
    s = useWorkspaceStore.getState()
    expect(s.tabs).toBe(before)
  })

  it('one-tab-per-path: openEntityTab upgrades a plain file tab in place (buffer kept, slot kept)', async () => {
    const { useWorkspaceStore } = await loadStore()
    const open = (p: string) => useWorkspaceStore.getState().openBuffer(p, { content: '' })
    open('a.yaml')
    open('pipelines/p/f.yaml')
    open('b.yaml')

    useWorkspaceStore.getState().openEntityTab('p', 'f', 'pipelines/p/f.yaml')
    const s = useWorkspaceStore.getState()
    // The file tab at the same path became the entity tab, in place.
    expect(s.tabs).toEqual([
      { kind: 'file', path: 'a.yaml' },
      {
        kind: 'entity',
        pipeline: 'p',
        flowgroup: 'f',
        filePath: 'pipelines/p/f.yaml',
        docKind: 'flowgroup',
        view: 'graph',
      },
      { kind: 'file', path: 'b.yaml' },
    ])
    expect(s.activePath).toBe('entity:p/f')
    // The buffer survives the upgrade (the YAML view reuses it).
    expect(s.buffers.map((b) => b.path)).toEqual(['a.yaml', 'pipelines/p/f.yaml', 'b.yaml'])
  })

  it('an upgraded entity tab restores without duplicating its buffer as a file tab', async () => {
    const first = await loadStore()
    first.useWorkspaceStore
      .getState()
      .openBuffer('pipelines/p/f.yaml', { content: 'x', etag: 'e1', exists: true })
    first.useWorkspaceStore.getState().openEntityTab('p', 'f', 'pipelines/p/f.yaml')

    const second = await loadStore({ keepStorage: true })
    const s = second.useWorkspaceStore.getState()
    expect(s.tabs).toEqual([
      {
        kind: 'entity',
        pipeline: 'p',
        flowgroup: 'f',
        filePath: 'pipelines/p/f.yaml',
        docKind: 'flowgroup',
        view: 'graph',
      },
    ])
    expect(s.activePath).toBe('entity:p/f')
  })

  it('openConfigTab is idempotent-focus and applies an explicit view on re-open', async () => {
    const { useWorkspaceStore } = await loadStore()
    useWorkspaceStore.getState().openConfigTab('config/pipeline_config_dev.yaml', 'pipeline')
    let s = useWorkspaceStore.getState()
    expect(s.tabs).toEqual([
      {
        kind: 'config',
        path: 'config/pipeline_config_dev.yaml',
        configKind: 'pipeline',
        view: 'form',
      },
    ])
    const configId = s.activePath
    expect(configId).toBe('config:config/pipeline_config_dev.yaml')

    // Re-open with no view change → single tab, no churn.
    const before = s.tabs
    useWorkspaceStore.getState().openConfigTab('config/pipeline_config_dev.yaml', 'pipeline')
    expect(useWorkspaceStore.getState().tabs).toBe(before)

    // Re-open requesting yaml → view switches in place.
    useWorkspaceStore
      .getState()
      .openConfigTab('config/pipeline_config_dev.yaml', 'pipeline', { view: 'yaml' })
    s = useWorkspaceStore.getState()
    expect(s.tabs).toHaveLength(1)
    expect(s.tabs[0]).toMatchObject({ kind: 'config', view: 'yaml' })
  })

  it('openProjectMap opens the singleton map tab and is idempotent', async () => {
    const { useWorkspaceStore } = await loadStore()
    useWorkspaceStore.getState().openProjectMap()
    let s = useWorkspaceStore.getState()
    expect(s.tabs).toEqual([{ kind: 'project-map' }])
    expect(s.activePath).toBe('project-map')

    useWorkspaceStore.getState().openBuffer('a.yaml', { content: '' })
    const before = useWorkspaceStore.getState().tabs
    useWorkspaceStore.getState().openProjectMap()
    s = useWorkspaceStore.getState()
    // No duplicate; focus returns to the existing map tab.
    expect(s.tabs).toBe(before)
    expect(s.activePath).toBe('project-map')
  })

  it('openPipelineDag opens a per-pipeline DAG tab keyed by pipeline and is idempotent', async () => {
    const { useWorkspaceStore, workspaceTabId } = await loadStore()
    useWorkspaceStore.getState().openPipelineDag('bronze')
    let s = useWorkspaceStore.getState()
    expect(s.tabs).toEqual([{ kind: 'pipeline-dag', pipeline: 'bronze' }])
    expect(s.activePath).toBe('pipeline-dag:bronze')
    // The namespaced id is derived by workspaceTabId (no collision across kinds).
    expect(workspaceTabId(s.tabs[0])).toBe('pipeline-dag:bronze')

    // Re-open the same pipeline while another tab is active → focus, no dup.
    useWorkspaceStore.getState().openBuffer('a.yaml', { content: '' })
    const before = useWorkspaceStore.getState().tabs
    useWorkspaceStore.getState().openPipelineDag('bronze')
    s = useWorkspaceStore.getState()
    expect(s.tabs).toBe(before)
    expect(s.activePath).toBe('pipeline-dag:bronze')

    // A different pipeline opens a second DAG tab.
    useWorkspaceStore.getState().openPipelineDag('silver')
    s = useWorkspaceStore.getState()
    expect(s.tabs).toHaveLength(3)
    expect(s.activePath).toBe('pipeline-dag:silver')
  })

  it('closeTab removes a pipeline-dag tab without touching buffers (no backing buffer)', async () => {
    const { useWorkspaceStore, workspaceTabId } = await loadStore()
    useWorkspaceStore.getState().openBuffer('a.yaml', { content: '' })
    useWorkspaceStore.getState().openPipelineDag('bronze')
    useWorkspaceStore.getState().closeTab('pipeline-dag:bronze')
    const s = useWorkspaceStore.getState()
    expect(s.tabs.map(workspaceTabId)).toEqual(['a.yaml'])
    expect(s.buffers).toHaveLength(1)
    expect(s.activePath).toBe('a.yaml')
  })

  it('openTableDetail opens a tab keyed by fqn and is idempotent', async () => {
    const { useWorkspaceStore } = await loadStore()
    useWorkspaceStore.getState().openTableDetail('main.bronze.orders')
    let s = useWorkspaceStore.getState()
    expect(s.tabs).toEqual([{ kind: 'table-detail', fqn: 'main.bronze.orders' }])
    expect(s.activePath).toBe('table:main.bronze.orders')

    const before = s.tabs
    useWorkspaceStore.getState().openTableDetail('main.bronze.orders')
    s = useWorkspaceStore.getState()
    expect(s.tabs).toBe(before)
    // A different fqn opens a second tab.
    useWorkspaceStore.getState().openTableDetail('main.silver.orders')
    expect(useWorkspaceStore.getState().tabs).toHaveLength(2)
  })

  it('openResourceTab opens a tab keyed by kind/filePath and refreshes the name in place', async () => {
    const { useWorkspaceStore } = await loadStore()
    useWorkspaceStore.getState().openResourceTab('preset', 'bronze_layer', 'presets/bronze.yaml')
    let s = useWorkspaceStore.getState()
    expect(s.tabs).toEqual([
      { kind: 'resource', resourceKind: 'preset', name: 'bronze_layer', filePath: 'presets/bronze.yaml' },
    ])
    expect(s.activePath).toBe('resource:preset:presets/bronze.yaml')

    // Re-open with a changed display name → same tab, name refreshed.
    useWorkspaceStore.getState().openResourceTab('preset', 'bronze_v2', 'presets/bronze.yaml')
    s = useWorkspaceStore.getState()
    expect(s.tabs).toHaveLength(1)
    expect(s.tabs[0]).toMatchObject({ name: 'bronze_v2' })

    // Re-open unchanged → zero churn.
    const before = s.tabs
    useWorkspaceStore.getState().openResourceTab('preset', 'bronze_v2', 'presets/bronze.yaml')
    expect(useWorkspaceStore.getState().tabs).toBe(before)
  })

  it('setTabView switches an entity tab view (idempotent; unknown id is a no-op)', async () => {
    const { useWorkspaceStore } = await loadStore()
    useWorkspaceStore.getState().openEntityTab('p', 'f', 'pipelines/p/f.yaml')
    const id = useWorkspaceStore.getState().activePath!
    // Cold-opened entity tabs default to the Graph view.
    expect(useWorkspaceStore.getState().tabs[0]).toMatchObject({ kind: 'entity', view: 'graph' })

    useWorkspaceStore.getState().setTabView(id, 'code')
    expect(useWorkspaceStore.getState().tabs[0]).toMatchObject({ kind: 'entity', view: 'code' })

    useWorkspaceStore.getState().setTabView(id, 'graph')
    expect(useWorkspaceStore.getState().tabs[0]).toMatchObject({ view: 'graph' })

    // A config-only view on an entity tab is ignored (no churn).
    let before = useWorkspaceStore.getState().tabs
    useWorkspaceStore.getState().setTabView(id, 'yaml')
    expect(useWorkspaceStore.getState().tabs).toBe(before)

    // Same view again → no churn; unknown id → no-op.
    before = useWorkspaceStore.getState().tabs
    useWorkspaceStore.getState().setTabView(id, 'graph')
    expect(useWorkspaceStore.getState().tabs).toBe(before)
    useWorkspaceStore.getState().setTabView('ghost:x/y', 'code')
    expect(useWorkspaceStore.getState().tabs).toBe(before)
  })

  it('renameEntityTab re-keys a flowgroup tab and remaps activePath', async () => {
    const { useWorkspaceStore, entityTabId } = await loadStore()
    useWorkspaceStore.getState().openEntityTab('bronze', 'orders', 'pipelines/bronze/orders.yaml')
    const oldId = entityTabId('bronze', 'orders')
    expect(useWorkspaceStore.getState().activePath).toBe(oldId)

    useWorkspaceStore.getState().renameEntityTab(oldId, 'orders_v2')
    const s = useWorkspaceStore.getState()
    const newId = entityTabId('bronze', 'orders_v2')
    expect(s.tabs[0]).toMatchObject({ kind: 'entity', flowgroup: 'orders_v2', filePath: 'pipelines/bronze/orders.yaml' })
    // Tab identity + activePath track the new name; the file path is unchanged.
    expect(s.activePath).toBe(newId)
  })

  it('renameEntityTab is a no-op for unknown ids, unchanged names, template tabs, and id collisions', async () => {
    const { useWorkspaceStore, entityTabId } = await loadStore()
    useWorkspaceStore.getState().openEntityTab('p', 'a', 'pipelines/p/a.yaml', { activate: false })
    useWorkspaceStore.getState().openEntityTab('p', 'b', 'pipelines/p/b.yaml', { activate: false })
    useWorkspaceStore
      .getState()
      .openEntityTab('', 'tpl', 'templates/t.yaml', { docKind: 'template', activate: false })

    const before = useWorkspaceStore.getState().tabs
    // Unknown id, unchanged name, and template tab → no churn.
    useWorkspaceStore.getState().renameEntityTab('ghost:x/y', 'z')
    useWorkspaceStore.getState().renameEntityTab(entityTabId('p', 'a'), 'a')
    useWorkspaceStore.getState().renameEntityTab('entity:tpl:templates/t.yaml', 'other')
    expect(useWorkspaceStore.getState().tabs).toBe(before)
    // Renaming 'a' → 'b' would collide with the open b tab → refused.
    useWorkspaceStore.getState().renameEntityTab(entityTabId('p', 'a'), 'b')
    expect(useWorkspaceStore.getState().tabs).toBe(before)
  })

  it('setTabView on a config tab ignores entity views (graph/code) but accepts form/yaml', async () => {
    const { useWorkspaceStore } = await loadStore()
    useWorkspaceStore.getState().openConfigTab('config/job_config.yaml', 'job')
    const id = useWorkspaceStore.getState().activePath!

    // Config surfaces have no graph/code view — both are ignored, no churn.
    const before = useWorkspaceStore.getState().tabs
    useWorkspaceStore.getState().setTabView(id, 'graph')
    expect(useWorkspaceStore.getState().tabs).toBe(before)
    useWorkspaceStore.getState().setTabView(id, 'code')
    expect(useWorkspaceStore.getState().tabs).toBe(before)

    useWorkspaceStore.getState().setTabView(id, 'yaml')
    expect(useWorkspaceStore.getState().tabs[0]).toMatchObject({ kind: 'config', view: 'yaml' })
  })

  it('openBuffer on an upgraded entity-tab path focuses the entity tab and never duplicates', async () => {
    const { useWorkspaceStore, workspaceTabId } = await loadStore()
    useWorkspaceStore
      .getState()
      .openBuffer('pipelines/p/f.yaml', { content: 'x', etag: 'e1', exists: true })
    useWorkspaceStore.getState().openEntityTab('p', 'f', 'pipelines/p/f.yaml')
    // The file tab upgraded in place; the buffer is retained under its path.
    expect(useWorkspaceStore.getState().tabs).toHaveLength(1)
    expect(useWorkspaceStore.getState().activePath).toBe('entity:p/f')

    // Focus elsewhere, then re-open the buffer by its raw path.
    useWorkspaceStore.getState().openBuffer('other.yaml', { content: '' })
    expect(useWorkspaceStore.getState().activePath).toBe('other.yaml')
    useWorkspaceStore.getState().openBuffer('pipelines/p/f.yaml', { content: 'CLOBBER' })

    const s = useWorkspaceStore.getState()
    // Focus lands on the entity tab id, NOT the raw path (which resolves to no tab).
    expect(s.activePath).toBe('entity:p/f')
    // No second file tab spawned for a path the entity tab already claims.
    expect(s.tabs.map(workspaceTabId)).toEqual(['entity:p/f', 'other.yaml'])
    // The existing buffer is never clobbered by the re-open seed.
    expect(s.buffers.find((b) => b.path === 'pipelines/p/f.yaml')?.content).toBe('x')
  })

  it('openBuffer on a migrated entity tab with no buffer loads it and focuses the entity tab (no file tab)', async () => {
    localStorage.clear()
    localStorage.setItem(
      'lhp-workspace',
      JSON.stringify({
        state: {
          buffers: [],
          tabs: [
            {
              kind: 'designer',
              id: 'designer:bronze/orders',
              pipeline: 'bronze',
              flowgroup: 'orders',
              filePath: 'pipelines/bronze/orders.yaml',
            },
          ],
          activePath: 'designer:bronze/orders',
          projectRoot: '/p',
        },
        version: 0,
      }),
    )
    const { useWorkspaceStore } = await loadStore({ keepStorage: true })
    // Migrated to an entity tab that carries NO buffer yet.
    expect(useWorkspaceStore.getState().buffers).toHaveLength(0)
    expect(useWorkspaceStore.getState().tabs[0]).toMatchObject({ kind: 'entity', pipeline: 'bronze' })

    // Opening the buffer for that path loads it and focuses the entity tab —
    // it must NOT append a second (file) tab for the claimed path.
    useWorkspaceStore
      .getState()
      .openBuffer('pipelines/bronze/orders.yaml', { content: 'x', etag: 'e1', exists: true })
    const s = useWorkspaceStore.getState()
    expect(s.buffers.map((b) => b.path)).toEqual(['pipelines/bronze/orders.yaml'])
    expect(s.tabs).toHaveLength(1)
    expect(s.tabs[0]).toMatchObject({ kind: 'entity', pipeline: 'bronze' })
    expect(s.activePath).toBe('entity:bronze/orders')
  })

  it('openEntityTab upgrade with activate:false remaps a dangling activePath off the upgraded tab', async () => {
    const { useWorkspaceStore } = await loadStore()
    const open = (p: string) => useWorkspaceStore.getState().openBuffer(p, { content: '' })

    // The upgraded file tab WAS active: activePath must follow it to the entity
    // id (its raw-path id no longer resolves), even though activate:false.
    open('pipelines/a/x.yaml')
    expect(useWorkspaceStore.getState().activePath).toBe('pipelines/a/x.yaml')
    useWorkspaceStore.getState().openEntityTab('a', 'x', 'pipelines/a/x.yaml', { activate: false })
    expect(useWorkspaceStore.getState().activePath).toBe('entity:a/x')
    expect(useWorkspaceStore.getState().tabs[0]).toMatchObject({ kind: 'entity', pipeline: 'a' })

    // A BACKGROUND file tab upgraded with activate:false must not steal focus.
    open('pipelines/b/y.yaml')
    useWorkspaceStore.getState().setActive('entity:a/x')
    useWorkspaceStore.getState().openEntityTab('b', 'y', 'pipelines/b/y.yaml', { activate: false })
    expect(useWorkspaceStore.getState().activePath).toBe('entity:a/x')
    expect(useWorkspaceStore.getState().tabs.map((t) => t.kind)).toEqual(['entity', 'entity'])
  })
})

describe('workspaceStore — closeTab (uniform tab close)', () => {
  it('closeTab on an active entity tab focuses the neighbour and drops its backing buffer', async () => {
    const { useWorkspaceStore, workspaceTabId } = await loadStore()
    useWorkspaceStore.getState().openBuffer('a.yaml', { content: '' })
    // Open the buffer, then upgrade its file tab to an entity tab in place.
    useWorkspaceStore.getState().openBuffer('pipelines/p/f.yaml', { content: 'x', exists: true })
    useWorkspaceStore.getState().openEntityTab('p', 'f', 'pipelines/p/f.yaml')
    useWorkspaceStore.getState().openBuffer('c.yaml', { content: '' })
    useWorkspaceStore.getState().setActive('entity:p/f')

    useWorkspaceStore.getState().closeTab('entity:p/f')
    const s = useWorkspaceStore.getState()
    // The neighbour that slid into the closed slot takes focus.
    expect(s.activePath).toBe('c.yaml')
    expect(s.tabs.map(workspaceTabId)).toEqual(['a.yaml', 'c.yaml'])
    // The entity tab's backing buffer is removed (nothing leaks into persist).
    expect(s.buffers.some((b) => b.path === 'pipelines/p/f.yaml')).toBe(false)
  })

  it('closeTab on a file tab matches closeBuffer (buffer + tab gone, neighbour focused)', async () => {
    const { useWorkspaceStore } = await loadStore()
    useWorkspaceStore.getState().openBuffer('a.yaml', { content: '' })
    useWorkspaceStore.getState().openBuffer('b.yaml', { content: '' })
    useWorkspaceStore.getState().openBuffer('c.yaml', { content: '' })
    useWorkspaceStore.getState().setActive('b.yaml')

    useWorkspaceStore.getState().closeTab('b.yaml')
    const s = useWorkspaceStore.getState()
    expect(s.activePath).toBe('c.yaml')
    expect(s.buffers.map((b) => b.path)).toEqual(['a.yaml', 'c.yaml'])
  })

  it('closeTab removes a non-buffer tab without touching buffers, and ignores unknown ids', async () => {
    const { useWorkspaceStore, workspaceTabId } = await loadStore()
    useWorkspaceStore.getState().openBuffer('a.yaml', { content: '' })
    useWorkspaceStore.getState().openProjectMap()

    useWorkspaceStore.getState().closeTab('project-map')
    let s = useWorkspaceStore.getState()
    expect(s.tabs.map(workspaceTabId)).toEqual(['a.yaml'])
    // The map tab owns no buffer — the buffer list is untouched.
    expect(s.buffers).toHaveLength(1)
    expect(s.activePath).toBe('a.yaml')

    // Unknown id → no-op (no state churn).
    const before = useWorkspaceStore.getState().tabs
    useWorkspaceStore.getState().closeTab('ghost:x')
    s = useWorkspaceStore.getState()
    expect(s.tabs).toBe(before)
  })
})
