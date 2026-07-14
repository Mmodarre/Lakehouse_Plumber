import { useEffect } from 'react'
import { create } from 'zustand'
import type { YAMLError } from 'yaml'

import {
  parseFlowgroupFile,
  serializeFlowgroupFile,
  type FlowgroupFileHandle,
} from '../lib/flowgroup-doc'
import { parseConfigFile, serializeConfigFile, type ConfigFileHandle } from '../lib/yaml-doc'
import { useLayoutStore } from './layoutStore'
import { useWorkspaceStore } from './workspaceStore'
import type { DocKind } from './workspaceStore'

// ── documentStore — the entity-document sync engine (§6.1) ───
//
// A NON-persisted companion to workspaceStore. `workspaceStore.buffers`
// remains the single owner of raw text / etag / dirty / persistence (its
// quota-safe partialize is load-bearing); this store owns the NON-serializable
// comment-preserving parse handles (a CST handle must never enter a persisted
// store) and keeps them synced with the buffer text.
//
// Sync invariant (tested): `serialize(handle) === buffer.content` whenever
// `errors.length === 0`.
//
// Three sync directions, one echo-suppression token (`lastSerializedText`):
//  • Form/Graph → text: `mutate` runs the mutator on the KEPT handle
//    (byte-surgical), serializes, records the serialized text as the echo
//    token, pushes it into the buffer via `workspaceStore.updateContent`, and
//    bumps `version` WITHOUT re-parsing (the handle is already correct).
//  • Monaco/YAML → handle: the YAML view's debounced content capture (in
//    CenterArea) calls `reparse(path, text)`; `reparse` no-ops when
//    `text === lastSerializedText` so a mutate's own echo does not re-parse.
//  • Post-save re-anchor: `useWorkspaceSave.saveBuffer` calls `reparse` with
//    the saved text after every save (echo-suppressed in the common case;
//    genuinely re-anchors on a merge / take-theirs whose text differs).
//
// Degraded mode (risk 5): a reparse whose text has parse errors surfaces those
// errors but KEEPS the last-good handle (never nulls it) so Form/Graph can
// render a dimmed projection; `mutate` returns false; the YAML text stays
// editable throughout. On the initial `open`, there is no last-good handle
// yet, so a buffer that starts broken opens with the (error-carrying) parse.

/** A comment-preserving parse handle — flowgroup/template or config kinds. */
export type EntityHandle = FlowgroupFileHandle | ConfigFileHandle

/** One open entity document, layered over its workspaceStore buffer (§6.1). */
export interface EntityDocument {
  path: string
  kind: DocKind
  /** The comment-preserving parse handle. In practice NEVER null for an open
   * document: `open` always parses the buffer into a handle, and broken YAML
   * yields an error-carrying handle (not null). Non-empty `errors` keeps the
   * LAST-GOOD handle (degraded mode) rather than nulling it. The real
   * "not-yet-editable" gate is the buffer's loading state, not a null handle.
   * (`| null` survives only as the absent-document default in projections.) */
  handle: EntityHandle | null
  /** Parse errors of the most recent text; non-empty ⇒ degraded. */
  errors: readonly YAMLError[]
  /** Bumped on mutate AND on a non-echo reparse — the ONLY memo key. */
  version: number
  /** The text the current handle serializes to — the echo-suppression token. */
  lastSerializedText: string | null
}

interface DocumentStoreState {
  /** Open documents keyed by project-relative path. */
  docs: Record<string, EntityDocument>
  /** Parse the CURRENT workspaceStore buffer content for `path` as `kind`.
   * No-op when a document is already open at `path` with the same kind. */
  open: (path: string, kind: DocKind) => void
  /** Replace the handle from `text`. No-op when `text === lastSerializedText`
   * (echo). Clean parse ⇒ new handle; broken parse ⇒ keep last-good handle,
   * surface errors. Bumps `version` and records `text` as the echo token. */
  reparse: (path: string, text: string) => void
  /** Run `fn` on the kept handle (byte-surgical), serialize, push the text into
   * the buffer, bump `version` — WITHOUT re-parsing. Returns false when the
   * document is absent, not yet parsed, or degraded (parse errors).
   *
   * `fn` SHOULD be atomic: on success the whole mutation is committed; on a
   * throw NOTHING reaches the buffer. If a COMPOSITE `fn` applies some edits
   * and then throws, those partial edits are discarded by re-anchoring the
   * handle to the current buffer text, and `mutate` returns false. Either way
   * the sync invariant `serialize(handle) === buffer.content` is preserved. */
  mutate: (path: string, fn: (handle: EntityHandle) => void) => boolean
  /** Drop the document (its buffer, if any, stays owned by workspaceStore). */
  close: (path: string) => void
}

const NO_ERRORS: readonly YAMLError[] = []

function isFlowgroupKind(kind: DocKind): boolean {
  return kind === 'flowgroup' || kind === 'template'
}

/** Parse `text` into the handle type `kind` dictates (never throws — YAML
 * errors surface on `handle.errors`). */
function parseFor(kind: DocKind, text: string): EntityHandle {
  return isFlowgroupKind(kind) ? parseFlowgroupFile(text) : parseConfigFile(text)
}

/** Serialize `handle` with the serializer matching `kind` (byte-identical to
 * the parsed source when unmutated). */
function serializeFor(kind: DocKind, handle: EntityHandle): string {
  return isFlowgroupKind(kind)
    ? serializeFlowgroupFile(handle as FlowgroupFileHandle)
    : serializeConfigFile(handle as ConfigFileHandle)
}

function bufferContent(path: string): string {
  return useWorkspaceStore.getState().buffers.find((b) => b.path === path)?.content ?? ''
}

export const useDocumentStore = create<DocumentStoreState>()((set, get) => ({
  docs: {},

  open: (path, kind) => {
    const existing = get().docs[path]
    if (existing && existing.kind === kind) return
    const content = bufferContent(path)
    const handle = parseFor(kind, content)
    const doc: EntityDocument = {
      path,
      kind,
      handle,
      errors: handle.errors,
      // A same-path re-open under a different kind must move `version` so
      // version-keyed memos recompute; a genuinely fresh open starts at 0.
      version: existing ? existing.version + 1 : 0,
      lastSerializedText: content,
    }
    set((s) => ({ docs: { ...s.docs, [path]: doc } }))
  },

  reparse: (path, text) => {
    const existing = get().docs[path]
    if (!existing) return
    if (text === existing.lastSerializedText) return
    const parsed = parseFor(existing.kind, text)
    const clean = parsed.errors.length === 0
    const doc: EntityDocument = {
      ...existing,
      // Degraded: keep the last-good projection so Form/Graph can render dimmed.
      handle: clean ? parsed : existing.handle,
      errors: parsed.errors,
      version: existing.version + 1,
      lastSerializedText: text,
    }
    set((s) => ({ docs: { ...s.docs, [path]: doc } }))
  },

  mutate: (path, fn) => {
    const existing = get().docs[path]
    if (!existing || existing.handle === null) return false
    if (existing.errors.length > 0) return false
    try {
      fn(existing.handle)
    } catch (err) {
      // A single yaml-doc primitive validates (assertMutable) before it touches
      // the handle, so one throw leaves the handle untouched. A COMPOSITE
      // mutator, though, can apply an earlier edit and then throw, leaving the
      // handle out of sync with the buffer — a break that is otherwise
      // unrecoverable (neither buffer.content nor lastSerializedText changed, so
      // no later reparse fires). Re-anchor the handle to the current buffer text
      // (bypassing the echo token, since the buffer text is unchanged) so the
      // invariant `serialize(handle) === buffer.content` holds again, then
      // report failure without pushing any partial edit into the buffer.
      console.debug('[documentStore] mutate rejected, resyncing handle:', err)
      const content = bufferContent(path)
      const resynced = parseFor(existing.kind, content)
      const clean = resynced.errors.length === 0
      set((s) => ({
        docs: {
          ...s.docs,
          [path]: {
            ...existing,
            handle: clean ? resynced : existing.handle,
            errors: resynced.errors,
            version: existing.version + 1,
            lastSerializedText: content,
          },
        },
      }))
      return false
    }
    const text = serializeFor(existing.kind, existing.handle)
    const doc: EntityDocument = {
      // Handle identity is preserved (it was mutated in place).
      ...existing,
      version: existing.version + 1,
      lastSerializedText: text,
    }
    // Record the echo token BEFORE pushing text so a synchronous capture-reparse
    // triggered by updateContent no-ops.
    set((s) => ({ docs: { ...s.docs, [path]: doc } }))
    useWorkspaceStore.getState().updateContent(path, text)
    return true
  },

  close: (path) =>
    set((s) => {
      if (!(path in s.docs)) return {}
      const docs = { ...s.docs }
      delete docs[path]
      return { docs }
    }),
}))

/** Why an entity document is read-only, or null when it is editable. */
export type ReadOnlyReason = 'parse-error' | 'viewer' | 'loading'

/** Live projection of the entity document at `path` for a Form/Graph view. */
export interface EntityDocumentView {
  doc: EntityDocument | null
  handle: EntityHandle | null
  errors: readonly YAMLError[]
  version: number
  readOnly: boolean
  readOnlyReason: ReadOnlyReason | null
}

/**
 * Bind a mounted view to the entity document at `path`.
 *
 * The caller (which knows the kind) must have `open(path, kind)`-ed the
 * document; this hook reflects it and keeps the parse handle synced with the
 * buffer: whenever the buffer content diverges from the handle's last text it
 * reparses (idempotent / echo-suppressed). That covers the initial content
 * load, an external take-theirs, and a tab-switch flush that skipped
 * CenterArea's capture reparse. `readOnly` is the union of degraded parse,
 * the layoutStore viewer lens, and not-yet-loaded.
 */
export function useEntityDocument(path: string): EntityDocumentView {
  const doc = useDocumentStore((s) => s.docs[path]) ?? null
  const viewerMode = useLayoutStore((s) => s.viewerMode)
  const buffer = useWorkspaceStore((s) => s.buffers.find((b) => b.path === path) ?? null)

  useEffect(() => {
    if (!doc || !buffer || buffer.loading || buffer.loadFailed) return
    if (buffer.content !== doc.lastSerializedText) {
      useDocumentStore.getState().reparse(path, buffer.content)
    }
  }, [path, doc, buffer])

  // "Loaded" requires the parse handle to reflect the CURRENT buffer text, not
  // merely a present, non-loading buffer. Right after a content fetch resolves
  // (or an external take-theirs) the buffer holds the new text while the handle
  // still projects the OLD text, until the reparse effect above fires. Gating
  // on `buffer.content === doc.lastSerializedText` keeps that one-frame window
  // in the 'loading' state, so a still-syncing doc never masquerades as a
  // genuinely-empty entity (e.g. GraphView's "No flowgroup here"). The reparse
  // effect makes any mismatch transient, so this can never wedge on 'loading'.
  const loaded =
    doc != null &&
    doc.handle != null &&
    buffer != null &&
    !buffer.loading &&
    !buffer.loadFailed &&
    buffer.content === doc.lastSerializedText
  let readOnlyReason: ReadOnlyReason | null = null
  if (!loaded) readOnlyReason = 'loading'
  else if ((doc?.errors.length ?? 0) > 0) readOnlyReason = 'parse-error'
  else if (viewerMode) readOnlyReason = 'viewer'

  return {
    doc,
    handle: doc?.handle ?? null,
    errors: doc?.errors ?? NO_ERRORS,
    version: doc?.version ?? 0,
    readOnly: readOnlyReason !== null,
    readOnlyReason,
  }
}
