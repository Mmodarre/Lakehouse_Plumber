import { create } from 'zustand'
import { createJSONStorage, persist } from 'zustand/middleware'

// ── workspaceStore — persistent editor buffers ───────────────
//
// Ordered list of open editor buffers + the active one, persisted to
// localStorage (NOT the server) so open tabs and unsaved edits survive a
// reload. Replaces the retired modal-editing state (`uiStore.openFile` and
// the flowgroup editor modal's local tab state).
//
// Re-render discipline (the P3 lesson): Monaco fires a dirty signal on every
// keystroke, so every setter here is a no-op when the requested value is
// already current — `setDirty(path, true)` changes state exactly once per
// clean→dirty transition. Buffer *content* is only synced into the store at
// capture points (debounced typing, tab switch, save, reload), and each sync
// replaces only that buffer's object. Shell components must subscribe via
// narrow selectors (`buffers.length > 0`, `activePath`) which stay
// referentially stable across those syncs, so typing re-renders nothing and
// a debounced content capture re-renders only the workspace editor itself.
//
// Tab union: the strip is an ordered `tabs` list across two kinds — file
// tabs reference their EditorBuffer by path, designer tabs carry only their
// identity {pipeline, flowgroup, filePath} (the canvas re-derives content).
// `tabs` owns strip order and close-focus semantics uniformly across kinds;
// `buffers` keeps owning file content/state so the per-keystroke patch
// discipline above is untouched.

export interface EditorBuffer {
  path: string
  /** Monaco language id derived from the file extension. */
  language: string
  /** Icon category for the tab strip ('yaml' | 'sql' | 'python' | 'schema' | 'expectations' | 'file'). */
  category: string
  content: string
  originalContent: string
  isDirty: boolean
  isSaving: boolean
  /** Optimistic-concurrency token (null = new file / no ETag yet). */
  etag: string | null
  exists: boolean
  /** Created from a non-existent reference (scaffold / added file). */
  isNew: boolean
  /** Content fetch still in flight (multi-file flowgroup open). */
  loading: boolean
  /** Content fetch failed — the buffer holds no real content, so editing and
   * saving are blocked until a retry succeeds (a save from this state would
   * clobber the on-disk file with ''). */
  loadFailed: boolean
}

/** Initial values for a buffer being opened (all optional). */
export interface BufferSeed {
  content?: string
  originalContent?: string
  etag?: string | null
  exists?: boolean
  isNew?: boolean
  isDirty?: boolean
  loading?: boolean
  category?: string
  /** Pass false to open in the background without focusing the buffer. */
  activate?: boolean
}

/** The entity documents a center tab can host (§6.1 EntityDocument.DocKind). */
export type DocKind = 'flowgroup' | 'template' | 'project' | 'pipeline_config' | 'job_config'
/** View switcher for a flowgroup entity tab (§6.2). 'code' hosts the
 * multi-file Code surface (its editable yaml sub-tab replaces the old 'yaml'
 * view); Form was retired for flowgroups. */
export type EntityView = 'graph' | 'code'
/** View switcher for a config entity tab (§6.2 — Form|YAML only, no graph/code). */
export type ConfigView = 'form' | 'yaml'
/** Config surface a ConfigTab edits (§6.2). */
export type ConfigKind = 'project' | 'pipeline' | 'job'
/** Resource kind a ResourceTab hosts (§3 RESOURCES). */
export type ResourceKind = 'preset' | 'template' | 'blueprint' | 'environment'

/**
 * @deprecated Superseded by {@link EntityTab} (view:'graph'). No live path
 * creates one — the persist `migrate` converts stored DesignerTab entries →
 * EntityTab{view:'graph'}, and CenterArea renders any residual one on GraphView.
 * Retained only for the Wave-4-reserved drill mini-graph
 * (`openDesignerTab`/`openDesignerTemplateTab`). Do not add new callers — open
 * flowgroups via {@link WorkspaceState.openEntityTab} instead.
 *
 * Non-file workspace tab hosting the per-flowgroup (or template) graph view.
 */
export interface DesignerTab {
  kind: 'designer'
  /** Strip-wide unique tab id: `designer:<pipeline>/<flowgroup>` (flowgroup)
   * or `designer:tpl:<filePath>` (template). */
  id: string
  /** '' for a template (which has no pipeline). */
  pipeline: string
  /** Flowgroup name, or the template name in template mode. */
  flowgroup: string
  /** Project-relative path of the YAML the canvas edits. */
  filePath: string
  /** Absent = flowgroup canvas; 'template' = template-authoring mode. */
  docKind?: 'flowgroup' | 'template'
}

/** Center tab for a flowgroup (or other) entity document, edited live-synced
 * across the Graph and Code views (§6.2). Its buffer (when loaded) is keyed by
 * `filePath` in `buffers`; the tab carries no `id` field — see
 * {@link entityTabId}. */
export interface EntityTab {
  kind: 'entity'
  /** '' for a template (which has no pipeline). */
  pipeline: string
  /** Flowgroup name, or the template name in template mode. */
  flowgroup: string
  /** Project-relative path of the YAML this entity edits. */
  filePath: string
  docKind: DocKind
  view: EntityView
}

/** Center tab for one of the three config surfaces (§6.2). */
export interface ConfigTab {
  kind: 'config'
  /** Project-relative path of the config file. */
  path: string
  configKind: ConfigKind
  view: ConfigView
}

/** Singleton center tab for the project-wide dependency map (§6.2 / D11). */
export interface MapTab {
  kind: 'project-map'
}

/** Center tab for one pipeline's flowgroup-level DAG — the drill target when a
 * pipeline node is clicked on the project map. One tab per pipeline. */
export interface PipelineDagTab {
  kind: 'pipeline-dag'
  /** The pipeline whose flowgroup graph this tab renders. */
  pipeline: string
}

/** Center tab for a produced table's detail + lineage view (§6.2 / D6). */
export interface TableTab {
  kind: 'table-detail'
  /** Fully-qualified name (or `sink:…`) of the produced dataset. */
  fqn: string
}

/** Center tab for a preset/template/blueprint/environment resource (§6.2). */
export interface ResourceTab {
  kind: 'resource'
  resourceKind: ResourceKind
  name: string
  /** Project-relative path of the resource file. */
  filePath: string
}

/** Ordered tab-strip entry for a file tab. The EditorBuffer keyed by `path`
 * in `buffers` carries all content/state — the ref only contributes order. */
export interface FileTabRef {
  kind: 'file'
  path: string
}

export type WorkspaceTabRef =
  | FileTabRef
  | DesignerTab
  | EntityTab
  | ConfigTab
  | MapTab
  | PipelineDagTab
  | TableTab
  | ResourceTab

/** Strip-wide unique id of a tab. File tabs are identified by their path;
 * every other kind is namespaced so ids never collide across kinds. */
export function workspaceTabId(tab: WorkspaceTabRef): string {
  switch (tab.kind) {
    case 'file':
      return tab.path
    case 'designer':
      return tab.id
    case 'entity':
      return tab.docKind === 'template'
        ? entityTemplateTabId(tab.filePath)
        : entityTabId(tab.pipeline, tab.flowgroup)
    case 'config':
      return `config:${tab.path}`
    case 'project-map':
      return 'project-map'
    case 'pipeline-dag':
      return `pipeline-dag:${tab.pipeline}`
    case 'table-detail':
      return `table:${tab.fqn}`
    case 'resource':
      return `resource:${tab.resourceKind}:${tab.filePath}`
  }
}

/** Underlying buffer path a tab backs (used to dedupe orphan-buffer recovery
 * on boot, and by the center region for the tab dirty-dot + close target),
 * or null for tabs that do not own a text buffer. View-agnostic — distinct
 * from CenterArea's view-aware `yamlBufferPathFor`. */
export function tabBufferPath(tab: WorkspaceTabRef): string | null {
  switch (tab.kind) {
    case 'file':
      return tab.path
    case 'entity':
      return tab.filePath
    case 'config':
      return tab.path
    case 'resource':
      return tab.filePath
    default:
      return null
  }
}

/** Entity tab id for a flowgroup (mirrors the retired {@link designerTabId}). */
export function entityTabId(pipeline: string, flowgroup: string): string {
  return `entity:${pipeline}/${flowgroup}`
}

/** Entity tab id for a template, keyed by its file path (a template has no
 * pipeline; the file is its true identity — two files sharing a declared
 * `name` still get distinct tabs). */
export function entityTemplateTabId(filePath: string): string {
  return `entity:tpl:${filePath}`
}

/**
 * @deprecated Use {@link entityTabId}. Retained for the pre-redesign shell.
 */
export function designerTabId(pipeline: string, flowgroup: string): string {
  return `designer:${pipeline}/${flowgroup}`
}

/**
 * @deprecated Use {@link entityTemplateTabId}. Retained for the pre-redesign
 * shell. Designer tab id for a template, keyed by its file path (a template
 * has no pipeline, and the file is its true identity — two files sharing a
 * declared `name` still get distinct tabs).
 */
export function designerTemplateTabId(filePath: string): string {
  return `designer:tpl:${filePath}`
}

const EXT_TO_LANGUAGE: Record<string, string> = {
  yaml: 'yaml',
  yml: 'yaml',
  py: 'python',
  sql: 'sql',
  ddl: 'sql',
  json: 'json',
  md: 'markdown',
}

/** Monaco language id for a project-relative path. */
export function languageForPath(path: string): string {
  const ext = path.split('.').pop()?.toLowerCase() ?? ''
  return EXT_TO_LANGUAGE[ext] ?? 'plaintext'
}

/** Tab-icon category for a path opened outside a flowgroup context. */
export function categoryForPath(path: string): string {
  const ext = path.split('.').pop()?.toLowerCase() ?? ''
  if (ext === 'yaml' || ext === 'yml') return 'yaml'
  if (ext === 'sql' || ext === 'ddl') return 'sql'
  if (ext === 'py') return 'python'
  if (ext === 'json') {
    return path.includes('expectations') ? 'expectations' : 'schema'
  }
  return 'file'
}

/** Paths the editor must never write to (mirrors the backend write guard). */
export function isReadOnlyPath(path: string): boolean {
  return (
    path.startsWith('generated/') ||
    path.startsWith('.git/') ||
    path.startsWith('.lhp/logs/') ||
    path.startsWith('.lhp/dependencies/') ||
    path === '.lhp_state.json'
  )
}

// zustand's persist middleware calls storage.setItem synchronously inside
// every set(); an unguarded QuotaExceededError would therefore throw out of
// store actions (debounced content captures, openBuffer clicks). Swallow
// storage failures — persistence is best-effort, the in-memory state is the
// source of truth for the session.
const safeStorage: Storage = {
  getItem: (name: string): string | null => {
    try {
      return window.localStorage.getItem(name)
    } catch {
      return null
    }
  },
  setItem: (name: string, value: string): void => {
    try {
      window.localStorage.setItem(name, value)
    } catch (err) {
      console.debug('[workspace] persist skipped (storage full/unavailable):', err)
    }
  },
  removeItem: (name: string): void => {
    try {
      window.localStorage.removeItem(name)
    } catch {
      // best-effort
    }
  },
  key: (index: number) => window.localStorage.key(index),
  clear: () => window.localStorage.clear(),
  get length() {
    return window.localStorage.length
  },
}

type BufferPatch = Partial<Omit<EditorBuffer, 'path'>>

/** Replace one buffer's fields; returns null when nothing would change so
 * callers can skip the state update entirely (no identity churn). */
function patchBuffer(
  buffers: EditorBuffer[],
  path: string,
  patch: BufferPatch,
): EditorBuffer[] | null {
  const idx = buffers.findIndex((b) => b.path === path)
  if (idx === -1) return null
  const current = buffers[idx]
  const keys = Object.keys(patch) as (keyof BufferPatch)[]
  if (keys.every((k) => current[k] === patch[k])) return null
  const next = buffers.slice()
  next[idx] = { ...current, ...patch }
  return next
}

/** Remove one tab entry by id. When it was active, focus the neighbour that
 * slid into its slot (falling back to the previous tab, then to page view) —
 * uniform across tab kinds. */
function removeTabEntry(
  s: Pick<WorkspaceState, 'tabs' | 'activePath'>,
  id: string,
): Pick<WorkspaceState, 'tabs' | 'activePath'> {
  const idx = s.tabs.findIndex((t) => workspaceTabId(t) === id)
  if (idx === -1) {
    return { tabs: s.tabs, activePath: s.activePath === id ? null : s.activePath }
  }
  const tabs = s.tabs.filter((_, i) => i !== idx)
  let activePath = s.activePath
  if (activePath === id) {
    const next = tabs[idx] ?? tabs[idx - 1]
    activePath = next ? workspaceTabId(next) : null
  }
  return { tabs, activePath }
}

/** Idempotent-focus opener for a tab with no mutable display fields (map,
 * table-detail): focus if already open, otherwise append. */
function upsertSimpleTab(
  s: Pick<WorkspaceState, 'tabs' | 'activePath'>,
  tab: WorkspaceTabRef,
  activate: boolean,
): Partial<WorkspaceState> {
  const id = workspaceTabId(tab)
  if (s.tabs.some((t) => workspaceTabId(t) === id)) {
    return activate && s.activePath !== id ? { activePath: id } : {}
  }
  return { tabs: [...s.tabs, tab], activePath: activate ? id : s.activePath }
}

interface WorkspaceState {
  buffers: EditorBuffer[]
  /** Ordered tab strip across kinds (file tabs reference `buffers` by path). */
  tabs: WorkspaceTabRef[]
  /** Active tab id — a file-buffer path or a namespaced tab id
   * (see {@link workspaceTabId}: entity/config/map/table/resource); null = page view. */
  activePath: string | null
  /** Project root the persisted buffers belong to (guards cross-project restore). */
  projectRoot: string | null
  /** Count of dirty buffers restored from localStorage on boot (one-time prompt). */
  restoredDirtyCount: number

  openBuffer: (path: string, seed?: BufferSeed) => void
  closeBuffer: (path: string) => void
  /** Close a tab by id regardless of kind (the strip's uniform close action).
   * File tabs go through {@link closeBuffer}; other buffer-backed tabs
   * (entity/config/resource) drop their backing buffer too so nothing leaks
   * into the persisted workspace; non-buffer tabs (project-map/table-detail/
   * designer) just lose their strip entry. Every kind gets removeTabEntry's
   * neighbour-focus when the closed tab was active. The dirty-prompt decision
   * stays UI-side — this only mutates. */
  closeTab: (id: string) => void
  /** Close every tab (designer tabs included) — the workspace reset. */
  closeAllBuffers: () => void
  setActive: (path: string | null) => void

  // ── center tab strip (§6.2) — idempotent-focus openers ──────
  /** Open (or just focus) a flowgroup/other entity tab. One-tab-per-path: a
   * plain file tab already open at `filePath` is upgraded in place. */
  openEntityTab: (
    pipeline: string,
    flowgroup: string,
    filePath: string,
    opts?: { docKind?: DocKind; view?: EntityView; activate?: boolean },
  ) => void
  /** Open (or just focus) a config surface tab. */
  openConfigTab: (
    path: string,
    configKind: ConfigKind,
    opts?: { view?: ConfigView; activate?: boolean },
  ) => void
  /** Open (or just focus) the singleton project-map tab. */
  openProjectMap: (opts?: { activate?: boolean }) => void
  /** Open (or just focus) a per-pipeline flowgroup-DAG tab. One tab per pipeline. */
  openPipelineDag: (pipeline: string, opts?: { activate?: boolean }) => void
  /** Open (or just focus) a table-detail tab for a produced dataset. */
  openTableDetail: (fqn: string, opts?: { activate?: boolean }) => void
  /** Open (or just focus) a resource (preset/template/blueprint/env) tab. */
  openResourceTab: (
    resourceKind: ResourceKind,
    name: string,
    filePath: string,
    opts?: { activate?: boolean },
  ) => void
  /** Switch the view of an entity (Graph|Code) or config (Form|YAML) tab
   * (idempotent). No-op for non-view tab kinds, unknown ids, a config view on
   * an entity tab, or an entity view on a config tab. */
  setTabView: (id: string, view: EntityView | ConfigView) => void
  /** Re-key a flowgroup entity tab after its `flowgroup:` was renamed: update
   * the tab's flowgroup field, re-derive its id, and remap `activePath` when it
   * pointed at the old id. No-op for non-entity/template tabs (templates key by
   * file path, not name), an unchanged name, or when the new id would collide
   * with another open tab. The backing buffer path (the file) is unchanged. */
  renameEntityTab: (id: string, newFlowgroup: string) => void

  /**
   * @deprecated Use {@link openEntityTab}. Open (or just focus — idempotent)
   * the designer canvas for a flowgroup (pre-redesign shell only).
   */
  openDesignerTab: (pipeline: string, flowgroup: string, filePath: string) => void
  /**
   * @deprecated Use {@link openEntityTab} with docKind:'template'. Open (or
   * just focus) the designer for a template file (pre-redesign shell only).
   */
  openDesignerTemplateTab: (templateName: string, filePath: string) => void
  /** @deprecated Use {@link closeBuffer}/tab close paths. Close a designer tab. */
  closeDesignerTab: (id: string) => void
  /** Sync editor content into the store; recomputes isDirty vs originalContent. */
  updateContent: (path: string, content: string) => void
  /** Idempotent per-keystroke dirty flag (no churn when already at value). */
  setDirty: (path: string, dirty: boolean) => void
  setSaving: (path: string, saving: boolean) => void
  /** After a successful save: new etag, saved content becomes the baseline. */
  setEtagAndBaseline: (path: string, etag: string | null, content: string) => void
  /** Replace a buffer from disk (412 reload / take-theirs). */
  replaceBuffer: (path: string, content: string, etag: string | null) => void
  /** Fill a loading placeholder after its content fetch resolves. No-op on
   * buffers that are not loading (never clobbers user edits). */
  markLoaded: (path: string, content: string, etag: string | null) => void
  markLoadFailed: (path: string) => void
  /** Drop unsaved changes everywhere: revert existing files, close unsaved new ones. */
  discardDirty: () => void
  ackRestore: () => void
  /** Clear persisted buffers when the served project root changes. */
  ensureProjectScope: (root: string) => void
}

export const useWorkspaceStore = create<WorkspaceState>()(
  persist(
    (set, get) => ({
      buffers: [],
      tabs: [],
      activePath: null,
      projectRoot: null,
      restoredDirtyCount: 0,

      openBuffer: (path, seed) =>
        set((s) => {
          // Resolve the tab that backs this path (reverse of tabBufferPath). A
          // non-file tab (an entity/config/resource tab that upgraded or
          // migrated from a file at this path) OWNS the path — focus its
          // namespaced id, never the raw path (no tab resolves to it) and never
          // a duplicate file tab.
          const backing = s.tabs.find((t) => tabBufferPath(t) === path)
          const focusId = backing ? workspaceTabId(backing) : path

          if (s.buffers.some((b) => b.path === path)) {
            // No-op-if-open: never clobber an open buffer, just focus its tab.
            if (seed?.activate === false || s.activePath === focusId) return {}
            return { activePath: focusId }
          }
          const content = seed?.content ?? ''
          const buffer: EditorBuffer = {
            path,
            language: languageForPath(path),
            category: seed?.category ?? categoryForPath(path),
            content,
            originalContent: seed?.originalContent ?? content,
            isDirty: seed?.isDirty ?? false,
            isSaving: false,
            etag: seed?.etag ?? null,
            exists: seed?.exists ?? false,
            isNew: seed?.isNew ?? false,
            loading: seed?.loading ?? false,
            loadFailed: false,
          }
          // A non-file tab already claims this path (e.g. a migrated
          // designer→entity tab with no buffer yet): load its buffer but keep
          // the existing tab — don't append a duplicate file ref.
          const tabs: WorkspaceTabRef[] = backing
            ? s.tabs
            : [...s.tabs, { kind: 'file', path }]
          return {
            buffers: [...s.buffers, buffer],
            tabs,
            activePath: seed?.activate === false ? s.activePath : focusId,
          }
        }),

      closeBuffer: (path) =>
        set((s) => {
          if (!s.buffers.some((b) => b.path === path)) return {}
          const buffers = s.buffers.filter((b) => b.path !== path)
          return { buffers, ...removeTabEntry(s, path) }
        }),

      closeTab: (id) => {
        const tab = get().tabs.find((t) => workspaceTabId(t) === id)
        if (!tab) return
        // File tabs reuse the existing buffer-close path unchanged.
        if (tab.kind === 'file') {
          get().closeBuffer(tab.path)
          return
        }
        set((s) => {
          const bp = tabBufferPath(tab)
          const base = removeTabEntry(s, id)
          // Buffer-backed entity/config/resource tabs also drop their backing
          // buffer; non-buffer tabs (project-map/table-detail/designer) just
          // lose their strip entry.
          return bp ? { ...base, buffers: s.buffers.filter((b) => b.path !== bp) } : base
        })
      },

      closeAllBuffers: () => set({ buffers: [], tabs: [], activePath: null }),

      setActive: (path) =>
        set((s) => {
          if (s.activePath === path) return {}
          if (path !== null && !s.tabs.some((t) => workspaceTabId(t) === path)) return {}
          return { activePath: path }
        }),

      openEntityTab: (pipeline, flowgroup, filePath, opts) =>
        set((s) => {
          const docKind: DocKind = opts?.docKind ?? 'flowgroup'
          const activate = opts?.activate !== false
          const id =
            docKind === 'template'
              ? entityTemplateTabId(filePath)
              : entityTabId(pipeline, flowgroup)

          const existingIdx = s.tabs.findIndex((t) => workspaceTabId(t) === id)
          if (existingIdx !== -1) {
            // Already open: never duplicate, just focus. Refresh identity
            // fields in place if they moved; the view only changes when a
            // view was explicitly requested (a plain re-open keeps the user's
            // current view). Zero churn when nothing changed.
            const existing = s.tabs[existingIdx]
            const patch: Partial<WorkspaceState> = {}
            if (existing.kind === 'entity') {
              const nextView = opts?.view ?? existing.view
              if (
                existing.pipeline !== pipeline ||
                existing.flowgroup !== flowgroup ||
                existing.filePath !== filePath ||
                existing.docKind !== docKind ||
                existing.view !== nextView
              ) {
                const tabs = s.tabs.slice()
                tabs[existingIdx] = {
                  ...existing,
                  pipeline,
                  flowgroup,
                  filePath,
                  docKind,
                  view: nextView,
                }
                patch.tabs = tabs
              }
            }
            if (activate && s.activePath !== id) patch.activePath = id
            return patch
          }

          const entityTab: EntityTab = {
            kind: 'entity',
            pipeline,
            flowgroup,
            filePath,
            docKind,
            view: opts?.view ?? 'graph',
          }
          // One-tab-per-path: upgrade a plain file tab at this path in place
          // (keeps its buffer — YAML view reuses it — and its strip slot).
          const fileIdx = s.tabs.findIndex((t) => t.kind === 'file' && t.path === filePath)
          if (fileIdx !== -1) {
            const tabs = s.tabs.slice()
            tabs[fileIdx] = entityTab
            // If we upgraded the active file tab but aren't activating, its old
            // id (the raw path) no longer resolves — remap activePath to the
            // entity id so it doesn't dangle into page view.
            const activePath = activate || s.activePath === filePath ? id : s.activePath
            return { tabs, activePath }
          }
          return { tabs: [...s.tabs, entityTab], activePath: activate ? id : s.activePath }
        }),

      openConfigTab: (path, configKind, opts) =>
        set((s) => {
          const activate = opts?.activate !== false
          const id = `config:${path}`
          const existingIdx = s.tabs.findIndex((t) => workspaceTabId(t) === id)
          if (existingIdx !== -1) {
            const existing = s.tabs[existingIdx]
            const patch: Partial<WorkspaceState> = {}
            if (existing.kind === 'config') {
              const nextView = opts?.view ?? existing.view
              if (existing.configKind !== configKind || existing.view !== nextView) {
                const tabs = s.tabs.slice()
                tabs[existingIdx] = { ...existing, configKind, view: nextView }
                patch.tabs = tabs
              }
            }
            if (activate && s.activePath !== id) patch.activePath = id
            return patch
          }
          const tab: ConfigTab = { kind: 'config', path, configKind, view: opts?.view ?? 'form' }
          return { tabs: [...s.tabs, tab], activePath: activate ? id : s.activePath }
        }),

      openProjectMap: (opts) =>
        set((s) => upsertSimpleTab(s, { kind: 'project-map' }, opts?.activate !== false)),

      openPipelineDag: (pipeline, opts) =>
        set((s) =>
          upsertSimpleTab(s, { kind: 'pipeline-dag', pipeline }, opts?.activate !== false),
        ),

      openTableDetail: (fqn, opts) =>
        set((s) => upsertSimpleTab(s, { kind: 'table-detail', fqn }, opts?.activate !== false)),

      openResourceTab: (resourceKind, name, filePath, opts) =>
        set((s) => {
          const activate = opts?.activate !== false
          const id = `resource:${resourceKind}:${filePath}`
          const existingIdx = s.tabs.findIndex((t) => workspaceTabId(t) === id)
          if (existingIdx !== -1) {
            // Focus; refresh the display name in place if it changed.
            const existing = s.tabs[existingIdx]
            const patch: Partial<WorkspaceState> = {}
            if (existing.kind === 'resource' && existing.name !== name) {
              const tabs = s.tabs.slice()
              tabs[existingIdx] = { ...existing, name }
              patch.tabs = tabs
            }
            if (activate && s.activePath !== id) patch.activePath = id
            return patch
          }
          const tab: ResourceTab = { kind: 'resource', resourceKind, name, filePath }
          return { tabs: [...s.tabs, tab], activePath: activate ? id : s.activePath }
        }),

      setTabView: (id, view) =>
        set((s) => {
          const idx = s.tabs.findIndex((t) => workspaceTabId(t) === id)
          if (idx === -1) return {}
          const tab = s.tabs[idx]
          if (tab.kind === 'entity') {
            // Entity tabs only have Graph|Code — ignore config views.
            if (view !== 'graph' && view !== 'code') return {}
            if (tab.view === view) return {}
            const tabs = s.tabs.slice()
            tabs[idx] = { ...tab, view }
            return { tabs }
          }
          if (tab.kind === 'config') {
            // Config surfaces only have Form|YAML — ignore graph/code.
            if (view !== 'form' && view !== 'yaml') return {}
            if (tab.view === view) return {}
            const tabs = s.tabs.slice()
            tabs[idx] = { ...tab, view }
            return { tabs }
          }
          return {}
        }),

      renameEntityTab: (id, newFlowgroup) =>
        set((s) => {
          const idx = s.tabs.findIndex((t) => workspaceTabId(t) === id)
          if (idx === -1) return {}
          const tab = s.tabs[idx]
          if (tab.kind !== 'entity' || tab.docKind === 'template') return {}
          if (tab.flowgroup === newFlowgroup) return {}
          const updated: EntityTab = { ...tab, flowgroup: newFlowgroup }
          const newId = workspaceTabId(updated)
          // Never clobber a different tab already occupying the new id.
          if (s.tabs.some((t, i) => i !== idx && workspaceTabId(t) === newId)) return {}
          const tabs = s.tabs.slice()
          tabs[idx] = updated
          return { tabs, activePath: s.activePath === id ? newId : s.activePath }
        }),

      openDesignerTab: (pipeline, flowgroup, filePath) =>
        set((s) => {
          const id = designerTabId(pipeline, flowgroup)
          const existing = s.tabs.find(
            (t): t is DesignerTab => t.kind === 'designer' && t.id === id,
          )
          if (existing) {
            // Already open: never duplicate, just focus it. Refresh the
            // filePath in place when the flowgroup file moved; zero state
            // churn when nothing changed.
            const patch: Partial<WorkspaceState> = {}
            if (existing.filePath !== filePath) {
              patch.tabs = s.tabs.map((t) => (t === existing ? { ...existing, filePath } : t))
            }
            if (s.activePath !== id) patch.activePath = id
            return patch
          }
          const tab: DesignerTab = { kind: 'designer', id, pipeline, flowgroup, filePath }
          return { tabs: [...s.tabs, tab], activePath: id }
        }),

      openDesignerTemplateTab: (templateName, filePath) =>
        set((s) => {
          const id = designerTemplateTabId(filePath)
          const existing = s.tabs.find(
            (t): t is DesignerTab => t.kind === 'designer' && t.id === id,
          )
          if (existing) {
            // Already open: focus it; refresh the display name in place when
            // the template's declared `name` changed. Zero churn otherwise.
            const patch: Partial<WorkspaceState> = {}
            if (existing.flowgroup !== templateName) {
              patch.tabs = s.tabs.map((t) =>
                t === existing ? { ...existing, flowgroup: templateName } : t,
              )
            }
            if (s.activePath !== id) patch.activePath = id
            return patch
          }
          const tab: DesignerTab = {
            kind: 'designer',
            id,
            pipeline: '',
            flowgroup: templateName,
            filePath,
            docKind: 'template',
          }
          return { tabs: [...s.tabs, tab], activePath: id }
        }),

      closeDesignerTab: (id) =>
        set((s) => {
          if (!s.tabs.some((t) => t.kind === 'designer' && t.id === id)) return {}
          return removeTabEntry(s, id)
        }),

      updateContent: (path, content) =>
        set((s) => {
          const buf = s.buffers.find((b) => b.path === path)
          if (!buf) return {}
          const next = patchBuffer(s.buffers, path, {
            content,
            isDirty: content !== buf.originalContent,
          })
          return next ? { buffers: next } : {}
        }),

      setDirty: (path, dirty) =>
        set((s) => {
          const next = patchBuffer(s.buffers, path, { isDirty: dirty })
          return next ? { buffers: next } : {}
        }),

      setSaving: (path, saving) =>
        set((s) => {
          const next = patchBuffer(s.buffers, path, { isSaving: saving })
          return next ? { buffers: next } : {}
        }),

      setEtagAndBaseline: (path, etag, content) =>
        set((s) => {
          const next = patchBuffer(s.buffers, path, {
            etag,
            content,
            originalContent: content,
            isDirty: false,
            isSaving: false,
            exists: true,
            isNew: false,
          })
          return next ? { buffers: next } : {}
        }),

      replaceBuffer: (path, content, etag) =>
        set((s) => {
          const next = patchBuffer(s.buffers, path, {
            content,
            originalContent: content,
            etag,
            isDirty: false,
            isSaving: false,
            exists: true,
            loading: false,
            loadFailed: false,
          })
          return next ? { buffers: next } : {}
        }),

      markLoaded: (path, content, etag) =>
        set((s) => {
          const buf = s.buffers.find((b) => b.path === path)
          // Also accepted on load-failed buffers (the retry path); those hold
          // no user edits either, so filling them never clobbers anything.
          if (!buf || (!buf.loading && !buf.loadFailed)) return {}
          const next = patchBuffer(s.buffers, path, {
            content,
            originalContent: content,
            etag,
            isDirty: false,
            exists: true,
            loading: false,
            loadFailed: false,
          })
          return next ? { buffers: next } : {}
        }),

      markLoadFailed: (path) =>
        set((s) => {
          const next = patchBuffer(s.buffers, path, { loading: false, loadFailed: true })
          return next ? { buffers: next } : {}
        }),

      discardDirty: () =>
        set((s) => {
          if (!s.buffers.some((b) => b.isDirty)) return {}
          const buffers: EditorBuffer[] = []
          const dropped = new Set<string>()
          for (const b of s.buffers) {
            if (!b.isDirty) {
              buffers.push(b)
            } else if (b.isNew && !b.exists) {
              // Never saved — nothing to revert to; drop buffer AND its tab.
              dropped.add(b.path)
            } else {
              buffers.push({ ...b, content: b.originalContent, isDirty: false })
            }
          }
          const tabs = dropped.size
            ? s.tabs.filter((t) => t.kind !== 'file' || !dropped.has(t.path))
            : s.tabs
          // Designer tabs are never dirty, so an active one stays focused.
          const activePath =
            s.activePath !== null && tabs.some((t) => workspaceTabId(t) === s.activePath)
              ? s.activePath
              : null
          return { buffers, tabs, activePath }
        }),

      ackRestore: () => set({ restoredDirtyCount: 0 }),

      ensureProjectScope: (root) =>
        set((s) => {
          if (s.projectRoot === root) return {}
          if (s.projectRoot === null) return { projectRoot: root }
          // Restored tabs belong to a different project — drop them.
          return {
            projectRoot: root,
            buffers: [],
            tabs: [],
            activePath: null,
            restoredDirtyCount: 0,
          }
        }),
    }),
    {
      name: 'lhp-workspace',
      storage: createJSONStorage(() => safeStorage),
      // v1: the center tab strip gained typed entity/config/map/table/resource
      // kinds and DesignerTab retired into EntityTab. Convert any persisted
      // DesignerTab → EntityTab{view:'graph'} (graph = the designer's view) and
      // remap an activePath that pointed at a designer id to the new entity id.
      // v2: the flowgroup view model became Graph|Code (Form retired, the old
      // YAML view folded into Code). Remap each persisted EntityTab.view
      // 'form'|'graph'→'graph', 'yaml'→'code'; config tabs (Form|YAML) are
      // untouched. Migrations are staged so a v0 payload runs both passes.
      // Payloads with no `tabs` (pre-tab-union) are left for the boot-reconcile.
      version: 2,
      migrate: (persisted, version) => {
        let state = persisted as {
          tabs?: unknown[]
          activePath?: string | null
          [k: string]: unknown
        }
        if (!state || !Array.isArray(state.tabs)) return state

        // v0 → v1: DesignerTab → EntityTab{view:'graph'}.
        if (version < 1) {
          const idRemap = new Map<string, string>()
          const tabs = state.tabs.map((raw) => {
            const t = raw as Partial<DesignerTab> & { kind?: string }
            if (t?.kind !== 'designer') return raw
            const docKind: DocKind = t.docKind ?? 'flowgroup'
            const entity: EntityTab = {
              kind: 'entity',
              pipeline: t.pipeline ?? '',
              flowgroup: t.flowgroup ?? '',
              filePath: t.filePath ?? '',
              docKind,
              view: 'graph',
            }
            if (t.id) idRemap.set(t.id, workspaceTabId(entity))
            return entity
          })
          const activePath =
            typeof state.activePath === 'string' && idRemap.has(state.activePath)
              ? idRemap.get(state.activePath)!
              : (state.activePath ?? null)
          state = { ...state, tabs, activePath }
        }

        // v1 → v2: EntityTab.view 'form'|'graph' → 'graph', 'yaml' → 'code'.
        if (version < 2) {
          const tabs = (state.tabs as unknown[]).map((raw) => {
            const t = raw as { kind?: string; view?: string }
            if (t?.kind !== 'entity') return raw
            return { ...t, view: t.view === 'yaml' ? 'code' : 'graph' }
          })
          state = { ...state, tabs }
        }

        return state
      },
      // Persist content only for DIRTY buffers (unsaved edits must survive a
      // reload). Clean buffers that exist on disk restore as `loading`
      // placeholders and re-fetch on boot (WorkspaceEditor) — this keeps
      // large read-only files out of localStorage and kills stale-clean
      // restores. Clean buffers that never existed keep their (empty) text.
      // Non-file tabs persist whole — they are identity-only; their content
      // (when any) is re-derived from the buffer keyed by their file path.
      partialize: (s) => ({
        buffers: s.buffers.map((b) => {
          const base = { ...b, isSaving: false, loading: false, loadFailed: false }
          if (b.isDirty || !b.exists) return base
          return { ...base, content: '', originalContent: '', loading: true }
        }),
        tabs: s.tabs,
        activePath: s.activePath,
        projectRoot: s.projectRoot,
      }),
    },
  ),
)

// localStorage hydration is synchronous, so restored state is already in
// place here. Reconcile the tab strip with the restored buffers (payloads
// persisted before the tab union existed carry no `tabs` — rebuild file
// entries in buffer order; stray file entries without a buffer are dropped),
// flag restored dirty buffers (drives the one-time "restore unsaved
// changes?" prompt) and drop an activePath that no longer resolves to a tab.
{
  const s = useWorkspaceStore.getState()
  const bufferPaths = new Set(s.buffers.map((b) => b.path))
  const kept = s.tabs.filter((t) => t.kind !== 'file' || bufferPaths.has(t.path))
  // Buffers already claimed by a surviving tab (a file tab, or an
  // entity/config/resource tab that upgraded a file tab in place) must NOT
  // spawn a duplicate file ref. Only genuinely orphaned buffers (legacy
  // pre-tab-union payloads) get a rebuilt file entry, in buffer order.
  const claimedPaths = new Set(
    kept.map(tabBufferPath).filter((p): p is string => p !== null),
  )
  const tabs: WorkspaceTabRef[] = [
    ...kept,
    ...s.buffers
      .filter((b) => !claimedPaths.has(b.path))
      .map((b): FileTabRef => ({ kind: 'file', path: b.path })),
  ]
  const tabsChanged = kept.length !== s.tabs.length || tabs.length !== s.tabs.length
  const restoredDirty = s.buffers.filter((b) => b.isDirty).length
  const activeOk = s.activePath === null || tabs.some((t) => workspaceTabId(t) === s.activePath)
  if (restoredDirty > 0 || !activeOk || tabsChanged) {
    useWorkspaceStore.setState({
      tabs,
      restoredDirtyCount: restoredDirty,
      activePath: activeOk ? s.activePath : null,
    })
  }
}
