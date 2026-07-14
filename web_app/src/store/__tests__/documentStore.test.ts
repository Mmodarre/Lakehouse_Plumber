import { describe, expect, it, vi } from 'vitest'
import { act, renderHook, waitFor } from '@testing-library/react'

import type { FlowgroupFileHandle } from '@/lib/flowgroup-doc'
import type { ConfigFileHandle } from '@/lib/yaml-doc'
import type { DocKind } from '@/store/workspaceStore'

type Stores = Awaited<ReturnType<typeof loadStores>>

// documentStore reads workspaceStore buffers (which hydrate + run a fixup at
// module load) and useEntityDocument reads layoutStore, so every scenario
// imports a fresh module graph via vi.resetModules() + dynamic import — the
// same pattern as workspaceStore.test.ts. All modules resolve to one instance
// within a single reset cycle.
async function loadStores() {
  vi.resetModules()
  localStorage.clear()
  const workspace = await import('@/store/workspaceStore')
  const document = await import('@/store/documentStore')
  const flowgroup = await import('@/lib/flowgroup-doc')
  const yaml = await import('@/lib/yaml-doc')
  const layout = await import('@/store/layoutStore')
  return { workspace, document, flowgroup, yaml, layout }
}

const PATH = 'pipelines/bronze/orders.yaml'

// Realistic flowgroup YAML with comments, blank lines, and a typed source.
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

// A second, valid-but-different projection (an extra comment + changed value)
// standing in for on-disk text after a merge / take-theirs.
const FLOWGROUP_YAML_B = `# Bronze ingest for orders (edited on disk)
pipeline: bronze
flowgroup: orders

actions:
  # Load raw orders from cloud files
  - name: load_orders
    type: load
    source:
      type: cloudfiles
      path: /mnt/raw/orders
      format: parquet
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

// Broken YAML: an unterminated double-quoted scalar → a parse error.
const FLOWGROUP_YAML_BROKEN = `# Bronze ingest for orders
pipeline: bronze
flowgroup: "orders
`

const PROJECT_PATH = 'lhp.yaml'
const PROJECT_YAML = `# Project configuration
name: my_project
version: "1.0"
`

// A pipeline_config-style two-document YAML (`---` separated, with comments in
// both documents) standing in for a multi-pipeline config file.
const MULTI_CONFIG_PATH = 'pipeline_config.yaml'
const MULTI_CONFIG_YAML = `# Pipeline config: bronze layer
pipeline: bronze_ingest
configuration:
  # tuning knob for bronze
  max_files_per_trigger: "100"
---
# Pipeline config: silver layer
pipeline: silver_curate
configuration:
  # tuning knob for silver
  max_files_per_trigger: "200"
`

/** Seed a workspace buffer, then open the entity document at that path. */
function seedAndOpen(
  workspace: Stores['workspace'],
  document: Stores['document'],
  path: string,
  content: string,
  kind: DocKind,
) {
  workspace.useWorkspaceStore.getState().openBuffer(path, { content, exists: true })
  document.useDocumentStore.getState().open(path, kind)
}

describe('documentStore — open / handle dispatch by DocKind', () => {
  it('open parses the current buffer as a flowgroup handle and seeds the echo token', async () => {
    const { workspace, document, flowgroup } = await loadStores()
    seedAndOpen(workspace, document, PATH, FLOWGROUP_YAML, 'flowgroup')

    const doc = document.useDocumentStore.getState().docs[PATH]
    expect(doc.kind).toBe('flowgroup')
    expect(doc.errors).toHaveLength(0)
    expect(doc.version).toBe(0)
    expect(doc.lastSerializedText).toBe(FLOWGROUP_YAML)
    // The handle is a flowgroup handle → round-trips via serializeFlowgroupFile.
    expect(flowgroup.serializeFlowgroupFile(doc.handle as FlowgroupFileHandle)).toBe(FLOWGROUP_YAML)
  })

  it('open parses a config kind as a yaml-doc ConfigFileHandle', async () => {
    const { workspace, document, yaml } = await loadStores()
    seedAndOpen(workspace, document, PROJECT_PATH, PROJECT_YAML, 'project')

    const doc = document.useDocumentStore.getState().docs[PROJECT_PATH]
    expect(doc.kind).toBe('project')
    expect(doc.errors).toHaveLength(0)
    expect(yaml.serializeConfigFile(doc.handle as ConfigFileHandle)).toBe(PROJECT_YAML)
  })

  it('open is a no-op when already open at the same kind (no version churn)', async () => {
    const { workspace, document } = await loadStores()
    seedAndOpen(workspace, document, PATH, FLOWGROUP_YAML, 'flowgroup')
    const before = document.useDocumentStore.getState().docs[PATH]
    document.useDocumentStore.getState().open(PATH, 'flowgroup')
    expect(document.useDocumentStore.getState().docs[PATH]).toBe(before)
  })
})

describe('documentStore — mutate invariant (serialize === buffer.content)', () => {
  it('a setActionField mutate pushes serialized text into the buffer and preserves comments', async () => {
    const { workspace, document, flowgroup } = await loadStores()
    seedAndOpen(workspace, document, PATH, FLOWGROUP_YAML, 'flowgroup')

    const ok = document.useDocumentStore.getState().mutate(PATH, (handle) => {
      const fg = flowgroup.selectFlowgroup(handle as FlowgroupFileHandle, 'orders')!
      flowgroup.setActionField(fg, 'load_orders', ['source', 'format'], 'csv')
    })
    expect(ok).toBe(true)

    const doc = document.useDocumentStore.getState().docs[PATH]
    const buffer = workspace.useWorkspaceStore.getState().buffers.find((b) => b.path === PATH)!
    const serialized = flowgroup.serializeFlowgroupFile(doc.handle as FlowgroupFileHandle)

    // The sync invariant: serialize(handle) === buffer.content === echo token.
    expect(serialized).toBe(buffer.content)
    expect(doc.lastSerializedText).toBe(buffer.content)
    expect(doc.version).toBe(1)
    expect(buffer.isDirty).toBe(true)

    // Only the one scalar changed; every comment + untouched line survives.
    expect(buffer.content).toContain('format: csv')
    expect(buffer.content).not.toContain('format: json')
    expect(buffer.content).toContain('# Bronze ingest for orders')
    expect(buffer.content).toContain('# Load raw orders from cloud files')
    expect(buffer.content).toContain('# Persist to the bronze table')
    expect(buffer.content).toContain('    path: /mnt/raw/orders')
  })

  it('a config-kind setPath mutate round-trips through serializeConfigFile', async () => {
    const { workspace, document, yaml } = await loadStores()
    seedAndOpen(workspace, document, PROJECT_PATH, PROJECT_YAML, 'project')

    const ok = document.useDocumentStore.getState().mutate(PROJECT_PATH, (handle) => {
      yaml.setPath(handle as ConfigFileHandle, 0, ['name'], 'renamed_project')
    })
    expect(ok).toBe(true)

    const doc = document.useDocumentStore.getState().docs[PROJECT_PATH]
    const buffer = workspace.useWorkspaceStore
      .getState()
      .buffers.find((b) => b.path === PROJECT_PATH)!
    expect(yaml.serializeConfigFile(doc.handle as ConfigFileHandle)).toBe(buffer.content)
    expect(buffer.content).toContain('name: renamed_project')
    expect(buffer.content).toContain('# Project configuration')
  })

  it('a multi-document config mutate in docIndex>0 keeps BOTH documents byte-anchored', async () => {
    const { workspace, document, yaml } = await loadStores()
    seedAndOpen(workspace, document, MULTI_CONFIG_PATH, MULTI_CONFIG_YAML, 'pipeline_config')

    const opened = document.useDocumentStore.getState().docs[MULTI_CONFIG_PATH]
    expect(yaml.documentCount(opened.handle as ConfigFileHandle)).toBe(2)

    // Mutate a field in the SECOND document (docIndex 1) via the handle.
    const ok = document.useDocumentStore.getState().mutate(MULTI_CONFIG_PATH, (handle) => {
      yaml.setPath(handle as ConfigFileHandle, 1, ['pipeline'], 'silver_v2')
    })
    expect(ok).toBe(true)

    const doc = document.useDocumentStore.getState().docs[MULTI_CONFIG_PATH]
    const buffer = workspace.useWorkspaceStore
      .getState()
      .buffers.find((b) => b.path === MULTI_CONFIG_PATH)!

    // The sync invariant holds across the whole multi-document file.
    expect(yaml.serializeConfigFile(doc.handle as ConfigFileHandle)).toBe(buffer.content)
    expect(doc.lastSerializedText).toBe(buffer.content)

    // Only the second document's scalar changed; the first document is byte-
    // anchored and every comment in both documents survives.
    expect(buffer.content).toContain('pipeline: silver_v2')
    expect(buffer.content).not.toContain('pipeline: silver_curate')
    expect(buffer.content).toContain('pipeline: bronze_ingest')
    expect(buffer.content).toContain('# Pipeline config: bronze layer')
    expect(buffer.content).toContain('# Pipeline config: silver layer')
    expect(buffer.content).toContain('# tuning knob for bronze')
    expect(buffer.content).toContain('# tuning knob for silver')
    expect(buffer.content).toContain('\n---\n')
  })
})

describe('documentStore — composite-mutator resync (invariant on throw)', () => {
  it('a mutator that mutates-then-throws returns false AND resyncs the handle to the buffer', async () => {
    const { workspace, document, flowgroup } = await loadStores()
    seedAndOpen(workspace, document, PATH, FLOWGROUP_YAML, 'flowgroup')

    // A composite mutator that applies one byte-surgical edit to the handle
    // and THEN throws — the pre-fix hazard: the first edit stays on the handle
    // while the buffer keeps the old text, silently breaking the invariant.
    const ok = document.useDocumentStore.getState().mutate(PATH, (handle) => {
      const fg = flowgroup.selectFlowgroup(handle as FlowgroupFileHandle, 'orders')!
      flowgroup.setActionField(fg, 'load_orders', ['source', 'format'], 'csv')
      throw new Error('composite mutator failed after a partial edit')
    })
    expect(ok).toBe(false)

    const doc = document.useDocumentStore.getState().docs[PATH]
    const buffer = workspace.useWorkspaceStore.getState().buffers.find((b) => b.path === PATH)!

    // The partial edit was discarded: the handle re-anchored to the buffer, so
    // the invariant holds and the buffer never saw the aborted edit.
    expect(flowgroup.serializeFlowgroupFile(doc.handle as FlowgroupFileHandle)).toBe(buffer.content)
    expect(doc.lastSerializedText).toBe(buffer.content)
    expect(doc.errors).toHaveLength(0)
    expect(buffer.content).toBe(FLOWGROUP_YAML)
    expect(buffer.content).toContain('format: json')
    expect(buffer.content).not.toContain('format: csv')

    // A follow-up (well-behaved) mutate still works against the resynced handle.
    const ok2 = document.useDocumentStore.getState().mutate(PATH, (handle) => {
      const fg = flowgroup.selectFlowgroup(handle as FlowgroupFileHandle, 'orders')!
      flowgroup.setActionField(fg, 'load_orders', ['source', 'format'], 'csv')
    })
    expect(ok2).toBe(true)
    const after = document.useDocumentStore.getState().docs[PATH]
    const bufAfter = workspace.useWorkspaceStore.getState().buffers.find((b) => b.path === PATH)!
    expect(flowgroup.serializeFlowgroupFile(after.handle as FlowgroupFileHandle)).toBe(
      bufAfter.content,
    )
    expect(bufAfter.content).toContain('format: csv')
  })
})

describe('documentStore — echo suppression', () => {
  it('a capture reparse with the just-mutated text is a no-op (version + handle identity held)', async () => {
    const { workspace, document, flowgroup } = await loadStores()
    seedAndOpen(workspace, document, PATH, FLOWGROUP_YAML, 'flowgroup')

    document.useDocumentStore.getState().mutate(PATH, (handle) => {
      const fg = flowgroup.selectFlowgroup(handle as FlowgroupFileHandle, 'orders')!
      flowgroup.setActionField(fg, 'load_orders', ['source', 'format'], 'csv')
    })
    const afterMutate = document.useDocumentStore.getState().docs[PATH]
    const buffer = workspace.useWorkspaceStore.getState().buffers.find((b) => b.path === PATH)!

    // Simulate CenterArea's debounced capture reparse of the (identical) text.
    document.useDocumentStore.getState().reparse(PATH, buffer.content)

    const afterReparse = document.useDocumentStore.getState().docs[PATH]
    // Echo suppressed: version unchanged, handle identity preserved, no reparse.
    expect(afterReparse.version).toBe(afterMutate.version)
    expect(afterReparse.handle).toBe(afterMutate.handle)
    expect(afterReparse).toBe(afterMutate)
  })
})

describe('documentStore — degraded mode (parse errors)', () => {
  it('valid → invalid keeps the last-good handle, flips to degraded, and blocks mutate', async () => {
    const { workspace, document } = await loadStores()
    seedAndOpen(workspace, document, PATH, FLOWGROUP_YAML, 'flowgroup')
    const lastGood = document.useDocumentStore.getState().docs[PATH].handle

    // Transiently-invalid YAML (what the YAML view produces mid-keystroke).
    document.useDocumentStore.getState().reparse(PATH, FLOWGROUP_YAML_BROKEN)
    const degraded = document.useDocumentStore.getState().docs[PATH]
    expect(degraded.errors.length).toBeGreaterThan(0)
    // The last-good projection is retained (NOT nulled) so Form/Graph can dim.
    expect(degraded.handle).toBe(lastGood)
    expect(degraded.lastSerializedText).toBe(FLOWGROUP_YAML_BROKEN)

    // mutate is refused while degraded.
    const ok = document.useDocumentStore.getState().mutate(PATH, () => {
      throw new Error('mutator must not run while degraded')
    })
    expect(ok).toBe(false)
  })

  it('invalid → valid restores a clean handle and re-enables mutate', async () => {
    const { workspace, document, flowgroup } = await loadStores()
    seedAndOpen(workspace, document, PATH, FLOWGROUP_YAML, 'flowgroup')
    document.useDocumentStore.getState().reparse(PATH, FLOWGROUP_YAML_BROKEN)
    expect(document.useDocumentStore.getState().docs[PATH].errors.length).toBeGreaterThan(0)

    document.useDocumentStore.getState().reparse(PATH, FLOWGROUP_YAML_B)
    const restored = document.useDocumentStore.getState().docs[PATH]
    expect(restored.errors).toHaveLength(0)
    expect(flowgroup.serializeFlowgroupFile(restored.handle as FlowgroupFileHandle)).toBe(
      FLOWGROUP_YAML_B,
    )

    const ok = document.useDocumentStore.getState().mutate(PATH, (handle) => {
      const fg = flowgroup.selectFlowgroup(handle as FlowgroupFileHandle, 'orders')!
      flowgroup.setActionField(fg, 'load_orders', ['source', 'format'], 'csv')
    })
    expect(ok).toBe(true)
  })
})

describe('documentStore — post-save re-anchor (risk 11)', () => {
  it('reparse to differing on-disk text re-anchors byte-identically and stays surgical', async () => {
    const { workspace, document, flowgroup } = await loadStores()
    seedAndOpen(workspace, document, PATH, FLOWGROUP_YAML, 'flowgroup')

    // A merge / take-theirs save persists text that differs from the handle's
    // last projection — reparse must genuinely re-anchor (not echo-suppress).
    document.useDocumentStore.getState().reparse(PATH, FLOWGROUP_YAML_B)
    const reanchored = document.useDocumentStore.getState().docs[PATH]
    expect(reanchored.errors).toHaveLength(0)
    expect(reanchored.lastSerializedText).toBe(FLOWGROUP_YAML_B)
    expect(flowgroup.serializeFlowgroupFile(reanchored.handle as FlowgroupFileHandle)).toBe(
      FLOWGROUP_YAML_B,
    )

    // A follow-up mutate remains byte-surgical against the re-anchored text.
    const ok = document.useDocumentStore.getState().mutate(PATH, (handle) => {
      const fg = flowgroup.selectFlowgroup(handle as FlowgroupFileHandle, 'orders')!
      flowgroup.setActionField(fg, 'load_orders', ['target'], 'v_orders_new')
    })
    expect(ok).toBe(true)
    const doc = document.useDocumentStore.getState().docs[PATH]
    const buffer = workspace.useWorkspaceStore.getState().buffers.find((b) => b.path === PATH)!
    expect(flowgroup.serializeFlowgroupFile(doc.handle as FlowgroupFileHandle)).toBe(buffer.content)
    expect(buffer.content).toContain('target: v_orders_new')
    // Comments from the re-anchored source survive the surgical edit.
    expect(buffer.content).toContain('# Bronze ingest for orders (edited on disk)')
    expect(buffer.content).toContain('format: parquet')
  })

  it('reparse to identical saved text echo-suppresses (common save leaves the handle intact)', async () => {
    const { workspace, document, flowgroup } = await loadStores()
    seedAndOpen(workspace, document, PATH, FLOWGROUP_YAML, 'flowgroup')
    document.useDocumentStore.getState().mutate(PATH, (handle) => {
      const fg = flowgroup.selectFlowgroup(handle as FlowgroupFileHandle, 'orders')!
      flowgroup.setActionField(fg, 'load_orders', ['source', 'format'], 'csv')
    })
    const before = document.useDocumentStore.getState().docs[PATH]
    const savedContent = before.lastSerializedText as string

    // The common save writes exactly what the handle already serializes to.
    document.useDocumentStore.getState().reparse(PATH, savedContent)
    const after = document.useDocumentStore.getState().docs[PATH]
    expect(after).toBe(before)
    expect(flowgroup.serializeFlowgroupFile(after.handle as FlowgroupFileHandle)).toBe(savedContent)
  })
})

describe('documentStore — close / open-degraded lifecycle', () => {
  it('close drops the entry; reparse/mutate on a closed path are no-ops', async () => {
    const { workspace, document } = await loadStores()
    seedAndOpen(workspace, document, PATH, FLOWGROUP_YAML, 'flowgroup')
    expect(document.useDocumentStore.getState().docs[PATH]).toBeDefined()

    document.useDocumentStore.getState().close(PATH)
    expect(document.useDocumentStore.getState().docs[PATH]).toBeUndefined()

    // Absent path: reparse no-ops, mutate returns false.
    document.useDocumentStore.getState().reparse(PATH, FLOWGROUP_YAML_B)
    expect(document.useDocumentStore.getState().docs[PATH]).toBeUndefined()
    expect(document.useDocumentStore.getState().mutate(PATH, () => {})).toBe(false)
  })

  it('open on a buffer with parse errors starts degraded', async () => {
    const { workspace, document } = await loadStores()
    seedAndOpen(workspace, document, PATH, FLOWGROUP_YAML_BROKEN, 'flowgroup')
    const doc = document.useDocumentStore.getState().docs[PATH]
    expect(doc.errors.length).toBeGreaterThan(0)
    expect(doc.handle).not.toBeNull()
    expect(document.useDocumentStore.getState().mutate(PATH, () => {})).toBe(false)
  })
})

describe('useEntityDocument — readOnly chain + buffer sync', () => {
  it('reports loading, parse-error, viewer, then editable', async () => {
    const { workspace, document, layout } = await loadStores()

    // loading: buffer still loading.
    workspace.useWorkspaceStore.getState().openBuffer(PATH, { exists: true, loading: true })
    document.useDocumentStore.getState().open(PATH, 'flowgroup')
    let hook = renderHook(() => document.useEntityDocument(PATH))
    expect(hook.result.current.readOnlyReason).toBe('loading')
    expect(hook.result.current.readOnly).toBe(true)
    hook.unmount()

    // parse-error: broken content loaded.
    act(() => {
      workspace.useWorkspaceStore.getState().markLoaded(PATH, FLOWGROUP_YAML_BROKEN, 'e1')
      document.useDocumentStore.getState().reparse(PATH, FLOWGROUP_YAML_BROKEN)
    })
    hook = renderHook(() => document.useEntityDocument(PATH))
    await waitFor(() => expect(hook.result.current.readOnlyReason).toBe('parse-error'))
    hook.unmount()

    // editable: fix the YAML.
    act(() => {
      workspace.useWorkspaceStore.getState().replaceBuffer(PATH, FLOWGROUP_YAML, 'e2')
      document.useDocumentStore.getState().reparse(PATH, FLOWGROUP_YAML)
    })
    hook = renderHook(() => document.useEntityDocument(PATH))
    await waitFor(() => expect(hook.result.current.readOnly).toBe(false))
    expect(hook.result.current.readOnlyReason).toBeNull()
    hook.unmount()

    // viewer: the read-only lens forces read-only even on a clean doc.
    act(() => layout.useLayoutStore.getState().setViewerMode(true))
    hook = renderHook(() => document.useEntityDocument(PATH))
    await waitFor(() => expect(hook.result.current.readOnlyReason).toBe('viewer'))
    hook.unmount()
  })

  it('reparses when the buffer content diverges from the handle (initial-load catch-up)', async () => {
    const { workspace, document } = await loadStores()
    // Doc opened while the buffer was empty (Form view before content arrived).
    workspace.useWorkspaceStore.getState().openBuffer(PATH, { content: '', exists: true })
    document.useDocumentStore.getState().open(PATH, 'flowgroup')
    expect(document.useDocumentStore.getState().docs[PATH].lastSerializedText).toBe('')

    // Content arrives in the buffer; the mounted hook reparses it in an effect.
    act(() => workspace.useWorkspaceStore.getState().updateContent(PATH, FLOWGROUP_YAML))
    const hook = renderHook(() => document.useEntityDocument(PATH))
    await waitFor(() =>
      expect(document.useDocumentStore.getState().docs[PATH].lastSerializedText).toBe(
        FLOWGROUP_YAML,
      ),
    )
    expect(hook.result.current.errors).toHaveLength(0)
    hook.unmount()
  })
})
