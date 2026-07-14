import { describe, expect, it } from 'vitest'
import { act, renderHook, waitFor } from '@testing-library/react'

import type { FlowgroupFileHandle } from '@/lib/flowgroup-doc'

type Stores = Awaited<ReturnType<typeof loadStores>>

// useFlowgroupDoc reads workspaceStore buffers + documentStore parse handles +
// layoutStore (viewer lens), all of which hydrate at module load. Each scenario
// imports a fresh module graph via vi.resetModules() + dynamic import so every
// module resolves to one instance within a reset cycle (same pattern as
// documentStore.test.ts).
async function loadStores() {
  const { vi } = await import('vitest')
  vi.resetModules()
  localStorage.clear()
  const workspace = await import('@/store/workspaceStore')
  const document = await import('@/store/documentStore')
  const layout = await import('@/store/layoutStore')
  const flowgroup = await import('@/lib/flowgroup-doc')
  const entity = await import('../useFlowgroupDoc')
  return { workspace, document, layout, flowgroup, entity }
}

const PATH = 'pipelines/bronze/orders.yaml'

const FLOWGROUP_YAML = `# Bronze ingest for orders
pipeline: bronze
flowgroup: orders

actions:
  # Load raw orders from cloud files
  - name: load_orders
    type: load
    source:
      type: cloudfiles
      path: /mnt/raw/orders
      format: json
    target: v_orders_raw

  # Persist to the bronze table
  - name: write_orders
    type: write
    source: v_orders_raw
    write_target:
      type: streaming_table
      database: bronze
      table: orders
`

const FLOWGROUP_YAML_BROKEN = `# Bronze ingest for orders
pipeline: bronze
flowgroup: "orders
`

/** Seed a clean buffer then open the entity document at PATH as a flowgroup. */
function seedClean(workspace: Stores['workspace'], document: Stores['document'], content = FLOWGROUP_YAML) {
  workspace.useWorkspaceStore.getState().openBuffer(PATH, { content, exists: true })
  document.useDocumentStore.getState().open(PATH, 'flowgroup')
}

describe('useFlowgroupDoc — derivation', () => {
  it('derives doc / meta / actions / graph from a multi-action flowgroup', async () => {
    const { workspace, document, entity } = await loadStores()
    seedClean(workspace, document)

    const { result } = renderHook(() => entity.useFlowgroupDoc(PATH, 'flowgroup'))

    expect(result.current.readOnly).toBe(false)
    expect(result.current.readOnlyReason).toBeNull()
    expect(result.current.meta?.pipeline).toBe('bronze')
    expect(result.current.meta?.flowgroup).toBe('orders')
    expect(result.current.actions.map((a) => a.name)).toEqual(['load_orders', 'write_orders'])
    expect(result.current.actions[0].kind).toBe('load')
    expect(result.current.graph?.nodes).toHaveLength(2)
    expect(result.current.doc?.info.name).toBe('orders')
  })
})

describe('useFlowgroupDoc — commit', () => {
  it('a field commit round-trips: buffer updated, comments preserved, serialize === buffer', async () => {
    const { workspace, document, flowgroup, entity } = await loadStores()
    seedClean(workspace, document)
    const { result } = renderHook(() => entity.useFlowgroupDoc(PATH, 'flowgroup'))
    const v0 = result.current.version

    let ok = false
    act(() => {
      ok = result.current.commit((doc) =>
        flowgroup.setActionField(doc, 'load_orders', ['source', 'format'], 'csv'),
      )
    })
    expect(ok).toBe(true)

    const buffer = workspace.useWorkspaceStore.getState().buffers.find((b) => b.path === PATH)!
    const handle = document.useDocumentStore.getState().docs[PATH].handle as FlowgroupFileHandle
    expect(flowgroup.serializeFlowgroupFile(handle)).toBe(buffer.content)
    expect(buffer.content).toContain('format: csv')
    expect(buffer.content).not.toContain('format: json')
    expect(buffer.content).toContain('# Bronze ingest for orders')
    expect(buffer.content).toContain('# Load raw orders from cloud files')
    expect(buffer.isDirty).toBe(true)
    expect(result.current.version).toBe(v0 + 1)
  })

  it('rename mutates the flowgroup key and keeps the index-0 selection intact', async () => {
    const { workspace, document, entity } = await loadStores()
    seedClean(workspace, document)
    const { result } = renderHook(() => entity.useFlowgroupDoc(PATH, 'flowgroup'))

    act(() => {
      result.current.rename('orders_v2')
    })

    const buffer = workspace.useWorkspaceStore.getState().buffers.find((b) => b.path === PATH)!
    expect(buffer.content).toContain('flowgroup: orders_v2')
    expect(buffer.content).toContain('# Bronze ingest for orders')
    // Selecting by index (not name) keeps the same flowgroup projection.
    expect(result.current.meta?.flowgroup).toBe('orders_v2')
    expect(result.current.actions).toHaveLength(2)
  })
})

describe('useFlowgroupDoc — readOnly chain', () => {
  it('loading → editable → viewer → parse-error (degraded blocks commit)', async () => {
    const { workspace, document, layout, entity } = await loadStores()

    // loading: buffer content still in flight.
    workspace.useWorkspaceStore.getState().openBuffer(PATH, { exists: true, loading: true })
    document.useDocumentStore.getState().open(PATH, 'flowgroup')
    let hook = renderHook(() => entity.useFlowgroupDoc(PATH, 'flowgroup'))
    expect(hook.result.current.readOnlyReason).toBe('loading')
    expect(hook.result.current.readOnly).toBe(true)
    hook.unmount()

    // editable: content arrives.
    act(() => {
      workspace.useWorkspaceStore.getState().markLoaded(PATH, FLOWGROUP_YAML, 'e1')
      document.useDocumentStore.getState().reparse(PATH, FLOWGROUP_YAML)
    })
    hook = renderHook(() => entity.useFlowgroupDoc(PATH, 'flowgroup'))
    await waitFor(() => expect(hook.result.current.readOnly).toBe(false))
    expect(hook.result.current.readOnlyReason).toBeNull()
    hook.unmount()

    // viewer lens: read-only even on a clean doc.
    act(() => layout.useLayoutStore.getState().setViewerMode(true))
    hook = renderHook(() => entity.useFlowgroupDoc(PATH, 'flowgroup'))
    await waitFor(() => expect(hook.result.current.readOnlyReason).toBe('viewer'))
    hook.unmount()

    // degraded: parse errors → readOnly + commit refused (never partially applied).
    act(() => {
      layout.useLayoutStore.getState().setViewerMode(false)
      workspace.useWorkspaceStore.getState().updateContent(PATH, FLOWGROUP_YAML_BROKEN)
      document.useDocumentStore.getState().reparse(PATH, FLOWGROUP_YAML_BROKEN)
    })
    hook = renderHook(() => entity.useFlowgroupDoc(PATH, 'flowgroup'))
    await waitFor(() => expect(hook.result.current.readOnlyReason).toBe('parse-error'))
    let ok = true
    act(() => {
      ok = hook.result.current.commit(() => {
        throw new Error('mutator must not run while degraded')
      })
    })
    expect(ok).toBe(false)
    hook.unmount()
  })
})

describe('useFlowgroupDoc — first-mount reactivity (regression: empty-graph bug)', () => {
  it('resolves the doc when open() materialises the handle post-mount at version 0', async () => {
    const { workspace, document, entity } = await loadStores()

    // The first-flowgroup-in-a-fresh-session path: the buffer already holds the
    // real content, but the entity document is NOT yet opened. useFlowgroupDoc's
    // own effect calls open() AFTER render 1, which parses the real content
    // straight into a version-0 doc (a fresh open starts at 0) with NO reparse to
    // bump the version. Keying `derived` on `version` alone froze it on render
    // 1's null projection, so the graph rendered "No flowgroup here" forever.
    workspace.useWorkspaceStore
      .getState()
      .openBuffer(PATH, { content: FLOWGROUP_YAML, exists: true })
    expect(document.useDocumentStore.getState().docs[PATH]).toBeUndefined()

    const { result } = renderHook(() => entity.useFlowgroupDoc(PATH, 'flowgroup'))

    // The null→real handle transition must re-derive even though version stays 0.
    await waitFor(() => expect(result.current.doc).not.toBeNull())
    expect(result.current.actions.map((a) => a.name)).toEqual(['load_orders', 'write_orders'])
    expect(result.current.readOnlyReason).toBeNull()
    expect(result.current.version).toBe(0)
  })

  it('a genuinely no-flowgroup file (fully synced) reads as empty, not loading', async () => {
    const { workspace, document, entity } = await loadStores()

    // A file with no flowgroup at index 0 — the ONLY case that should surface the
    // "No flowgroup here" empty state. It is loaded + synced (buffer.content ===
    // lastSerializedText), so readOnlyReason must be null (empty), never
    // 'loading': that is the empty-vs-loading distinction the fix preserves.
    const NO_FLOWGROUP = '# just a comment, no flowgroup here\n'
    workspace.useWorkspaceStore.getState().openBuffer(PATH, { content: NO_FLOWGROUP, exists: true })
    document.useDocumentStore.getState().open(PATH, 'flowgroup')

    const { result } = renderHook(() => entity.useFlowgroupDoc(PATH, 'flowgroup'))

    await waitFor(() => expect(result.current.readOnlyReason).toBeNull())
    expect(result.current.doc).toBeNull()
    expect(result.current.readOnly).toBe(false)
  })
})

describe('useFlowgroupDoc — memoization on version', () => {
  it('holds the actions array identity while version is unchanged, replaces it on mutate', async () => {
    const { workspace, document, flowgroup, entity } = await loadStores()
    seedClean(workspace, document)
    const { result, rerender } = renderHook(() => entity.useFlowgroupDoc(PATH, 'flowgroup'))

    const actions0 = result.current.actions
    rerender()
    // No version change ⇒ same memoized array (never memo on the handle).
    expect(result.current.actions).toBe(actions0)

    act(() => {
      result.current.commit((doc) =>
        flowgroup.setActionField(doc, 'load_orders', ['target'], 'v_orders_new'),
      )
    })
    // A mutate bumps version ⇒ the memo recomputes a fresh array.
    expect(result.current.actions).not.toBe(actions0)
    expect(result.current.actions[0].target).toBe('v_orders_new')
  })
})
