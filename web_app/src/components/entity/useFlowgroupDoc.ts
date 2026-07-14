import { useCallback, useEffect, useMemo } from 'react'

import {
  deriveGraph,
  listActions,
  readFlowgroupMeta,
  readTemplateParams,
  selectFlowgroupAt,
  selectTemplate,
  setFlowgroupField,
} from '@/lib/flowgroup-doc'
import type {
  ActionRead,
  FlowgroupDocHandle,
  FlowgroupFileHandle,
  FlowgroupGraph,
  FlowgroupMeta,
  TemplateParamRead,
} from '@/lib/flowgroup-doc'
import { useDocumentStore, useEntityDocument } from '@/store/documentStore'
import type { EntityHandle, ReadOnlyReason } from '@/store/documentStore'
import type { DocKind } from '@/store/workspaceStore'

// ── useFlowgroupDoc — typed flowgroup binding over documentStore ─
//
// The read/write surface FlowgroupFormView (and later GraphView) binds to.
// It layers the flowgroup-doc typed readers + `deriveGraph` over the entity
// document's comment-preserving parse handle (owned by documentStore, §6.1),
// and exposes a single atomic `commit` that routes through
// `documentStore.mutate` (byte-surgical, live-synced into the buffer).
//
// Selection rule: a form view edits ONE flowgroup per file — the entry at
// file index 0 (`selectFlowgroupAt(file, 0)`), or the template body for a
// template file. Selecting by INDEX (not by name) means renaming the
// `flowgroup:` key never loses the selection. Multi-flowgroup files show
// their first entry (a documented v1 limitation — §6.6 risk 4).
//
// Memoization discipline (§6.1): the parse handle's object identity is
// PRESERVED across an in-place `mutate` — only `version` changes (it is bumped
// on every mutate AND every non-echo reparse). So all derived data is memoized
// on `version`, NEVER on the handle: memoing on the handle would miss a
// post-mutate content change and freeze the form on stale actions.

/** A single, synchronous flowgroup-doc mutation applied to the selected
 * flowgroup (structurally identical to the designer's `DesignerMutator`, so it
 * plugs straight into `ActionForm`'s `commit` prop). */
export type FlowgroupMutator = (doc: FlowgroupDocHandle) => void

export interface FlowgroupDocApi {
  /** The selected flowgroup (file index 0) or template body. `null` until the
   * buffer has parsed cleanly at least once (loading / no-flowgroup / a broken
   * file with no last-good projection). */
  doc: FlowgroupDocHandle | null
  /** Typed flowgroup metadata, `null` when `doc` is `null`. */
  meta: FlowgroupMeta | null
  /** The flowgroup's actions in YAML order (stable identity while `version` is
   * unchanged — the memo key). Empty when `doc` is `null`. */
  actions: readonly ActionRead[]
  /** The derived action graph (producers/consumers/externals), `null` when
   * `doc` is `null`. */
  graph: FlowgroupGraph | null
  /** Declared template parameters, in YAML order — non-empty only for a
   * `template` document (always empty for a flowgroup). Edited via `commit`
   * with the `addTemplateParam` / `setTemplateParamField` / … mutators, which
   * address the same template body `doc`. */
  params: readonly TemplateParamRead[]
  /**
   * Apply ONE atomic flowgroup-doc mutation and live-sync it into the buffer.
   * `fn` MUST be a single synchronous flowgroup-doc mutator invocation (e.g.
   * `setActionField(doc, …)`); it runs against the kept handle, which is
   * serialized once and pushed into the buffer WITHOUT a re-parse. Returns
   * `false` when the document is absent, not yet loaded, or degraded (parse
   * errors) — the mutation is refused, never partially applied.
   */
  commit: (fn: FlowgroupMutator) => boolean
  /** Rename the flowgroup (or template) — a pure single-scalar mutation of the
   * `flowgroup:` (`name:` for templates) key. Returns `commit`'s result. */
  rename: (newName: string) => boolean
  /** Degraded ∨ viewer-lens ∨ buffer-loading (there is NO dirty block — §6.1). */
  readOnly: boolean
  readOnlyReason: ReadOnlyReason | null
  /** The entity document's version — the sole memo/change signal. */
  version: number
}

const EMPTY_ACTIONS: readonly ActionRead[] = []
const EMPTY_PARAMS: readonly TemplateParamRead[] = []

/** Select the one editable flowgroup/template a form view binds to. */
function selectEntity(handle: EntityHandle, docKind: DocKind): FlowgroupDocHandle | undefined {
  const file = handle as FlowgroupFileHandle
  return docKind === 'template' ? selectTemplate(file)?.body : selectFlowgroupAt(file, 0)
}

export function useFlowgroupDoc(filePath: string, docKind: DocKind): FlowgroupDocApi {
  // Idempotent: parses the current buffer if not already open at this kind.
  useEffect(() => {
    useDocumentStore.getState().open(filePath, docKind)
  }, [filePath, docKind])

  const { handle, version, readOnly, readOnlyReason } = useEntityDocument(filePath)

  const derived = useMemo(() => {
    const empty = {
      doc: null as FlowgroupDocHandle | null,
      meta: null as FlowgroupMeta | null,
      actions: EMPTY_ACTIONS,
      graph: null as FlowgroupGraph | null,
      params: EMPTY_PARAMS,
    }
    if (handle === null) return empty
    const selected = selectEntity(handle, docKind)
    if (selected === undefined) return empty
    // Template params are read from the full template handle (same body doc),
    // fresh on every version bump — empty for a flowgroup.
    let params: readonly TemplateParamRead[] = EMPTY_PARAMS
    if (docKind === 'template') {
      const template = selectTemplate(handle as FlowgroupFileHandle)
      if (template !== undefined) params = readTemplateParams(template)
    }
    return {
      doc: selected,
      meta: readFlowgroupMeta(selected),
      actions: listActions(selected) as readonly ActionRead[],
      graph: deriveGraph(selected),
      params,
    }
    // §6.1: BOTH `handle` and `version` are memo keys. `handle` catches the
    // transitions where a NEW handle object appears: a fresh `open` (absent
    // doc → real handle) and a clean `reparse`. This is load-bearing on first
    // mount — `open` runs in an effect AFTER render 1 (where the doc is still
    // absent and `version` defaults to 0), and when the buffer is already
    // loaded `open` parses the real content straight into a `version 0` doc
    // with NO reparse to bump it. `version` therefore stays 0 across the
    // null→real handle swap, so keying on `version` alone leaves `derived`
    // frozen on the null projection until a re-mount (the empty-graph bug).
    // `version` (bumped on every mutate AND reparse) is still required for an
    // in-place `mutate`, which PRESERVES the handle's object identity — a
    // handle-only key would miss it. `filePath` distinguishes tabs whose
    // versions coincide.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [version, handle, docKind, filePath])

  const commit = useCallback(
    (fn: FlowgroupMutator): boolean =>
      useDocumentStore.getState().mutate(filePath, (h) => {
        const selected = selectEntity(h, docKind)
        if (selected === undefined) {
          // mutate() swallows this and returns false (nothing was touched).
          throw new Error(`No editable ${docKind} at index 0 in ${filePath}`)
        }
        fn(selected)
      }),
    [filePath, docKind],
  )

  const rename = useCallback(
    (newName: string): boolean =>
      commit((doc) =>
        setFlowgroupField(doc, [docKind === 'template' ? 'name' : 'flowgroup'], newName),
      ),
    [commit, docKind],
  )

  return {
    doc: derived.doc,
    meta: derived.meta,
    actions: derived.actions,
    graph: derived.graph,
    params: derived.params,
    commit,
    rename,
    readOnly,
    readOnlyReason,
    version,
  }
}
