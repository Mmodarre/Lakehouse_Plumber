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
