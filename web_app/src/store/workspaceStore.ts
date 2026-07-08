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

interface WorkspaceState {
  buffers: EditorBuffer[]
  activePath: string | null
  /** Project root the persisted buffers belong to (guards cross-project restore). */
  projectRoot: string | null
  /** Count of dirty buffers restored from localStorage on boot (one-time prompt). */
  restoredDirtyCount: number

  openBuffer: (path: string, seed?: BufferSeed) => void
  closeBuffer: (path: string) => void
  closeAllBuffers: () => void
  setActive: (path: string | null) => void
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
    (set) => ({
      buffers: [],
      activePath: null,
      projectRoot: null,
      restoredDirtyCount: 0,

      openBuffer: (path, seed) =>
        set((s) => {
          if (s.buffers.some((b) => b.path === path)) {
            // No-op-if-open: never clobber an open buffer, just focus it.
            if (seed?.activate === false || s.activePath === path) return {}
            return { activePath: path }
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
          return {
            buffers: [...s.buffers, buffer],
            activePath: seed?.activate === false ? s.activePath : path,
          }
        }),

      closeBuffer: (path) =>
        set((s) => {
          const idx = s.buffers.findIndex((b) => b.path === path)
          if (idx === -1) return {}
          const buffers = s.buffers.filter((b) => b.path !== path)
          let activePath = s.activePath
          if (activePath === path) {
            // Focus the neighbour that slid into the closed tab's slot,
            // falling back to the previous tab, then to page view.
            activePath = buffers[idx]?.path ?? buffers[idx - 1]?.path ?? null
          }
          return { buffers, activePath }
        }),

      closeAllBuffers: () => set({ buffers: [], activePath: null }),

      setActive: (path) =>
        set((s) => {
          if (s.activePath === path) return {}
          if (path !== null && !s.buffers.some((b) => b.path === path)) return {}
          return { activePath: path }
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
          for (const b of s.buffers) {
            if (!b.isDirty) {
              buffers.push(b)
            } else if (b.isNew && !b.exists) {
              // Never saved — nothing to revert to; drop the buffer.
              continue
            } else {
              buffers.push({ ...b, content: b.originalContent, isDirty: false })
            }
          }
          const activePath = buffers.some((b) => b.path === s.activePath) ? s.activePath : null
          return { buffers, activePath }
        }),

      ackRestore: () => set({ restoredDirtyCount: 0 }),

      ensureProjectScope: (root) =>
        set((s) => {
          if (s.projectRoot === root) return {}
          if (s.projectRoot === null) return { projectRoot: root }
          // Restored buffers belong to a different project — drop them.
          return { projectRoot: root, buffers: [], activePath: null, restoredDirtyCount: 0 }
        }),
    }),
    {
      name: 'lhp-workspace',
      storage: createJSONStorage(() => safeStorage),
      // Persist content only for DIRTY buffers (unsaved edits must survive a
      // reload). Clean buffers that exist on disk restore as `loading`
      // placeholders and re-fetch on boot (WorkspaceEditor) — this keeps
      // large read-only files out of localStorage and kills stale-clean
      // restores. Clean buffers that never existed keep their (empty) text.
      partialize: (s) => ({
        buffers: s.buffers.map((b) => {
          const base = { ...b, isSaving: false, loading: false, loadFailed: false }
          if (b.isDirty || !b.exists) return base
          return { ...base, content: '', originalContent: '', loading: true }
        }),
        activePath: s.activePath,
        projectRoot: s.projectRoot,
      }),
    },
  ),
)

// localStorage hydration is synchronous, so restored state is already in
// place here. Flag restored dirty buffers (drives the one-time "restore
// unsaved changes?" prompt) and drop an activePath that no longer resolves.
{
  const s = useWorkspaceStore.getState()
  const restoredDirty = s.buffers.filter((b) => b.isDirty).length
  const activeOk = s.activePath === null || s.buffers.some((b) => b.path === s.activePath)
  if (restoredDirty > 0 || !activeOk) {
    useWorkspaceStore.setState({
      restoredDirtyCount: restoredDirty,
      activePath: activeOk ? s.activePath : null,
    })
  }
}
