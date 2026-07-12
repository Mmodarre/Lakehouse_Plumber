import { lazy, Suspense, useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { useLocation } from 'react-router-dom'
import { toast } from 'sonner'
import { Loader2, Lock, Minimize2 } from 'lucide-react'
import { useDirtyGuardSource } from '../../store/dirtyGuardStore'
import { useWorkspaceStore, isReadOnlyPath } from '../../store/workspaceStore'
import type { DesignerTab } from '../../store/workspaceStore'
import { useRunStore } from '../../store/runStore'
import { Badge } from '../ui/badge'
import { Button } from '../ui/button'
import { Input } from '../ui/input'
import { cn } from '../../lib/utils'
import { DiscardChangesDialog } from '../editor/DiscardChangesDialog'
import { ConflictDialog } from '../editor/ConflictDialog'
import { isYamlPath } from '../editor/yamlSaveSupport'
import { useBeforeUnloadGuard } from '../../hooks/useBeforeUnloadGuard'
import { EditorTabBar } from './EditorTabBar'
import type { WorkspaceTabInfo } from './EditorTabBar'
import { ADD_FILE_OPTIONS, loadBufferContent } from './flowgroupBuffers'
import { useWorkspaceSave } from './useWorkspaceSave'
import type { MonacoEditorHandle } from '../editor/MonacoEditorWrapper'

const MonacoEditorWrapper = lazy(() => import('../editor/MonacoEditorWrapper'))
const DesignerCanvas = lazy(() => import('../designer/DesignerCanvas'))

/** Debounce for syncing typed content into the (persisted) store. Keystrokes
 * themselves only flip the idempotent dirty flag — see workspaceStore. */
const CAPTURE_DEBOUNCE_MS = 500

function EditorSkeleton() {
  return (
    <div className="flex flex-1 items-center justify-center bg-card">
      <div className="h-6 w-6 animate-spin rounded-full border-2 border-border border-t-muted-foreground" />
    </div>
  )
}

/**
 * The persistent workspace: a docked tab strip + Monaco editor hosted in the
 * Layout center region (replaces the retired file/flowgroup editor modals).
 * The tab strip stays docked while tabs are open; the body shows Monaco when
 * a file buffer is focused, the designer canvas when a designer tab is
 * focused (activePath), and the routed page otherwise.
 */
export function WorkspaceEditor() {
  const buffers = useWorkspaceStore((s) => s.buffers)
  const tabs = useWorkspaceStore((s) => s.tabs)
  const activePath = useWorkspaceStore((s) => s.activePath)
  const setActive = useWorkspaceStore((s) => s.setActive)
  const openBuffer = useWorkspaceStore((s) => s.openBuffer)
  const closeBuffer = useWorkspaceStore((s) => s.closeBuffer)
  const closeDesignerTab = useWorkspaceStore((s) => s.closeDesignerTab)
  const updateContent = useWorkspaceStore((s) => s.updateContent)
  const setDirty = useWorkspaceStore((s) => s.setDirty)
  const discardDirty = useWorkspaceStore((s) => s.discardDirty)
  const restoredDirtyCount = useWorkspaceStore((s) => s.restoredDirtyCount)
  const ackRestore = useWorkspaceStore((s) => s.ackRestore)

  const editorRef = useRef<MonacoEditorHandle>(null)
  // Mirrors the currently-mounted buffer's path so async completions (412
  // reload, debounced capture) never target the wrong buffer mid-switch.
  const activePathRef = useRef<string | null>(activePath)
  const captureTimerRef = useRef<number | null>(null)

  const [pendingClose, setPendingClose] = useState<string | null>(null)
  const [addFilePath, setAddFilePath] = useState('')
  const [addFileCategory, setAddFileCategory] = useState('')

  // A designer tab id never collides with a buffer path, so at most one of
  // these resolves; `focused` covers both tab kinds.
  const activeBuffer = buffers.find((b) => b.path === activePath) ?? null
  const activeDesigner =
    tabs.find((t): t is DesignerTab => t.kind === 'designer' && t.id === activePath) ?? null
  const focused = activeBuffer !== null || activeDesigner !== null
  const isReadOnly = activeBuffer ? isReadOnlyPath(activeBuffer.path) : false
  // A buffer whose content fetch failed holds no real text — block editing
  // and saving until a retry succeeds so '' can never overwrite the file.
  const loadFailed = activeBuffer?.loadFailed ?? false
  const anyDirty = buffers.some((b) => b.isDirty)
  const dirtyCount = buffers.filter((b) => b.isDirty).length
  const anySaving = buffers.some((b) => b.isSaving)

  useEffect(() => {
    activePathRef.current = activePath
  }, [activePath])

  useBeforeUnloadGuard(anyDirty)

  // ── content capture ────────────────────────────────────────

  const cancelCapture = useCallback(() => {
    if (captureTimerRef.current !== null) {
      window.clearTimeout(captureTimerRef.current)
      captureTimerRef.current = null
    }
  }, [])

  /** Sync the live editor's text into the store (persisted to localStorage). */
  const captureActive = useCallback(() => {
    cancelCapture()
    const path = activePathRef.current
    if (path && editorRef.current) {
      updateContent(path, editorRef.current.getValue())
    }
  }, [cancelCapture, updateContent])

  const scheduleCapture = useCallback(() => {
    if (captureTimerRef.current !== null) window.clearTimeout(captureTimerRef.current)
    captureTimerRef.current = window.setTimeout(() => {
      captureTimerRef.current = null
      captureActive()
    }, CAPTURE_DEBOUNCE_MS)
  }, [captureActive])

  useEffect(() => cancelCapture, [cancelCapture])

  // Flush the live editor into the store right before unload so persisted
  // buffers carry the very latest keystrokes (localStorage writes are sync).
  useEffect(() => {
    window.addEventListener('beforeunload', captureActive)
    return () => window.removeEventListener('beforeunload', captureActive)
  }, [captureActive])

  const handleDirtyChange = useCallback(
    (dirty: boolean) => {
      const path = activePathRef.current
      if (!path) return
      if (dirty) {
        // Idempotent: flips store state once per clean→dirty transition.
        setDirty(path, true)
        scheduleCapture()
      } else {
        // Programmatic setValue (reload / take-theirs / discard). Every flow
        // that calls it re-baselines the store first, so the editor text now
        // equals the stored baseline — clearing the flag is safe and heals
        // any dirty flag left behind before the change-event suppression.
        cancelCapture()
        setDirty(path, false)
      }
    },
    [setDirty, scheduleCapture, cancelCapture],
  )

  // Flush the OUTGOING buffer on ANY activePath change — Minimize, external
  // setActive callers (FileBrowser / ProblemsPanel focusing an already-open
  // path), closing the active tab. Zustand subscribers run synchronously
  // inside set(), before React re-renders, so `editorRef` still holds the
  // outgoing buffer's editor and no typing burst inside the capture debounce
  // window is lost when its model is disposed.
  useEffect(() => {
    let prev = useWorkspaceStore.getState().activePath
    return useWorkspaceStore.subscribe((s) => {
      if (s.activePath === prev) return
      const outgoing = prev
      prev = s.activePath
      // Only flush while the mounted editor still shows the outgoing buffer:
      // handleTabSelect moves activePathRef (after capturing itself) before
      // calling setActive, so this fires only for external activePath moves.
      if (
        outgoing !== null &&
        activePathRef.current === outgoing &&
        editorRef.current &&
        s.buffers.some((b) => b.path === outgoing)
      ) {
        cancelCapture()
        updateContent(outgoing, editorRef.current.getValue())
      }
    })
  }, [updateContent, cancelCapture])

  // Fetch content for buffers restored from localStorage as `loading`
  // placeholders (clean buffers persist without their content — see the
  // workspaceStore partialize). Later placeholders (flowgroup opens) fetch
  // their own content; this only drains what boot restored.
  useEffect(() => {
    for (const b of useWorkspaceStore.getState().buffers) {
      if (b.loading) void loadBufferContent(b.path)
    }
  }, [])

  // ── save pipeline ──────────────────────────────────────────

  const {
    saveActive,
    saveAllDirty,
    conflict,
    cancelConflict,
    resolveKeepMine,
    resolveTakeTheirs,
  } = useWorkspaceSave({ editorRef, activePathRef, captureActive })

  // Re-derive YAML syntax markers from the synthetic Problems issue when an
  // editor (re)mounts: models are disposed on tab switch, so background
  // squiggles would otherwise vanish when switching back to the file.
  const handleEditorMount = useCallback(() => {
    const path = activePathRef.current
    if (!path || !isYamlPath(path)) return
    const issue = useRunStore
      .getState()
      .issues.find((i) => i.code === 'YAML-SYNTAX' && i.file_path === path)
    if (!issue) return
    const line = issue.context['line']
    const column = issue.context['column']
    if (typeof line !== 'number' || typeof column !== 'number') return
    editorRef.current?.setYamlMarkers([{ line, column, message: issue.title }])
  }, [])

  /** Retry a failed content fetch. On success the mounted editor still shows
   * the placeholder text (it is seeded via defaultValue), so push the fetched
   * content in via the programmatic (dirty-suppressed) setValue. */
  const handleRetryLoad = useCallback((path: string) => {
    void loadBufferContent(path).then(() => {
      const buf = useWorkspaceStore.getState().buffers.find((b) => b.path === path)
      if (buf && !buf.loadFailed && activePathRef.current === path) {
        editorRef.current?.setValue(buf.content)
      }
    })
  }, [])

  // ── restore prompt (one-time, non-blocking) ────────────────

  /** Drop unsaved changes and sync the mounted editor to the reverted text. */
  const applyDiscard = useCallback(() => {
    cancelCapture()
    discardDirty()
    const s = useWorkspaceStore.getState()
    const active = s.buffers.find((b) => b.path === s.activePath)
    if (active) editorRef.current?.setValue(active.content)
  }, [cancelCapture, discardDirty])

  useEffect(() => {
    if (restoredDirtyCount === 0) return
    ackRestore()
    toast.info(
      `Restored ${restoredDirtyCount} file(s) with unsaved changes from your last session`,
      {
        id: 'workspace-restore',
        duration: 10000,
        action: { label: 'Discard all', onClick: applyDiscard },
      },
    )
  }, [restoredDirtyCount, ackRestore, applyDiscard])

  // ── navigation guards ──────────────────────────────────────

  // A completed navigation switches the center back to the routed page;
  // clean buffers stay docked in the tab strip.
  const location = useLocation()
  const lastLocationKey = useRef(location.key)
  useEffect(() => {
    if (location.key === lastLocationKey.current) return
    lastLocationKey.current = location.key
    setActive(null)
  }, [location.key, setActive])

  // Route navigation while any buffer is dirty → confirm discard first.
  // The prompt itself lives in Layout's NavigationGuard (the app's single
  // useBlocker); this surface only contributes its dirty state + discard
  // action to the shared registry so other dirty surfaces (config forms)
  // are guarded in the SAME prompt instead of being silently dropped.
  const discardForNav = useCallback(() => {
    applyDiscard()
    setActive(null)
  }, [applyDiscard, setActive])

  useDirtyGuardSource(
    'workspace',
    useMemo(
      () =>
        anyDirty
          ? {
              message: `Unsaved changes in ${dirtyCount} file(s) will be lost.`,
              onDiscard: discardForNav,
            }
          : null,
      [anyDirty, dirtyCount, discardForNav],
    ),
  )

  // ── tab handlers ───────────────────────────────────────────

  const handleTabSelect = useCallback(
    (path: string) => {
      if (path === activePathRef.current) return
      // Capture the outgoing buffer BEFORE the mirror moves to the new path.
      captureActive()
      activePathRef.current = path
      setActive(path)
    },
    [captureActive, setActive],
  )

  const handleCloseTab = useCallback(
    (id: string) => {
      const buf = useWorkspaceStore.getState().buffers.find((b) => b.path === id)
      if (!buf) {
        // Designer tabs are never dirty — close without confirmation
        // (no-op for ids that aren't an open designer tab either).
        closeDesignerTab(id)
        return
      }
      if (buf.isDirty) {
        setPendingClose(id)
        return
      }
      cancelCapture()
      toast.dismiss(`stale:${id}`)
      closeBuffer(id)
    },
    [cancelCapture, closeBuffer, closeDesignerTab],
  )

  const handleConfirmedClose = useCallback(() => {
    if (pendingClose === null) return
    cancelCapture()
    toast.dismiss(`stale:${pendingClose}`)
    closeBuffer(pendingClose)
    setPendingClose(null)
  }, [pendingClose, cancelCapture, closeBuffer])

  // ── add-file (scaffold companion files) ────────────────────

  const handleAddFileSelect = useCallback(
    (category: string) => {
      const option = ADD_FILE_OPTIONS.find((o) => o.category === category)
      if (!option) return
      const activeName = activePathRef.current?.split('/').pop() ?? ''
      const stem = activeName.replace(/\.[^.]+$/, '') || 'new_file'
      setAddFileCategory(option.category)
      setAddFilePath(option.defaultPath(stem))
    },
    [],
  )

  const handleAddFileCancel = useCallback(() => {
    setAddFilePath('')
    setAddFileCategory('')
  }, [])

  const handleAddFileConfirm = useCallback(() => {
    const path = addFilePath.trim().replace(/^\/+/, '')
    if (!path || !addFileCategory) return
    if (useWorkspaceStore.getState().buffers.some((b) => b.path === path)) {
      toast.error('A tab with this path already exists')
      return
    }
    captureActive()
    openBuffer(path, {
      content: '',
      originalContent: '',
      isDirty: true,
      isNew: true,
      exists: false,
      category: addFileCategory,
    })
    handleAddFileCancel()
  }, [addFilePath, addFileCategory, captureActive, openBuffer, handleAddFileCancel])

  // ── render ─────────────────────────────────────────────────

  const pendingCloseName = pendingClose?.split('/').pop() ?? pendingClose

  // Join the ordered tab strip with buffer state (file tabs pull their
  // display fields from the buffer; a ref without a buffer never happens
  // post-boot-reconcile, but render nothing for it rather than crash).
  const tabInfos = tabs.flatMap((t): WorkspaceTabInfo[] => {
    if (t.kind === 'designer') {
      return [
        {
          kind: 'designer',
          id: t.id,
          pipeline: t.pipeline,
          flowgroup: t.flowgroup,
          docKind: t.docKind,
        },
      ]
    }
    const b = buffers.find((x) => x.path === t.path)
    return b
      ? [{ kind: 'file', path: b.path, category: b.category, exists: b.exists, isDirty: b.isDirty }]
      : []
  })

  return (
    <div className={cn('flex flex-col', focused && 'min-h-0 flex-1')}>
      {/* Tab strip row (always docked while tabs are open) */}
      <div className="flex items-center border-b border-border bg-sidebar">
        <EditorTabBar
          tabs={tabInfos}
          activePath={activePath}
          addFileOptions={ADD_FILE_OPTIONS}
          onSelectTab={handleTabSelect}
          onCloseTab={handleCloseTab}
          onAddFile={handleAddFileSelect}
        />
        <div className="flex shrink-0 items-center gap-2 px-3">
          {focused && isReadOnly && (
            <Badge
              variant="outline"
              className="h-5 rounded-sm px-1.5 text-2xs text-muted-foreground"
            >
              <Lock className="size-2.5" aria-hidden="true" />
              Read Only
            </Badge>
          )}
          {focused && activeBuffer && !isReadOnly && (
            <Button
              size="xs"
              onClick={saveActive}
              disabled={!activeBuffer.isDirty || activeBuffer.isSaving || loadFailed}
            >
              {activeBuffer.isSaving && <Loader2 className="animate-spin" aria-hidden="true" />}
              {activeBuffer.isSaving ? 'Saving...' : 'Save'}
            </Button>
          )}
          {dirtyCount > 1 && (
            <Button variant="outline" size="xs" onClick={saveAllDirty} disabled={anySaving}>
              Save All ({dirtyCount})
            </Button>
          )}
          {focused && (
            <Button
              variant="ghost"
              size="icon-xs"
              className="text-muted-foreground"
              aria-label="Hide editor and show the page"
              title="Hide editor"
              onClick={() => {
                // Flush the live editor before the model is disposed (the
                // store-subscribe flush also covers this; capturing here in
                // addition cancels the pending debounce explicitly).
                captureActive()
                setActive(null)
              }}
            >
              <Minimize2 aria-hidden="true" />
            </Button>
          )}
        </div>
      </div>

      {/* Add file path input (shown after selecting a file type) */}
      {addFileCategory && (
        <div className="flex items-center gap-2 border-b border-border bg-muted px-4 py-2">
          <span className="text-xs text-muted-foreground">Path:</span>
          <Input
            value={addFilePath}
            onChange={(e) => setAddFilePath(e.target.value)}
            className="h-7 flex-1 bg-card px-2 py-1 font-mono text-xs md:text-xs"
            autoFocus
            onKeyDown={(e) => {
              if (e.key === 'Enter') {
                e.preventDefault()
                handleAddFileConfirm()
              } else if (e.key === 'Escape') {
                e.preventDefault()
                handleAddFileCancel()
              }
            }}
          />
          <Button variant="outline" size="xs" onClick={handleAddFileConfirm}>
            Add
          </Button>
          <Button variant="ghost" size="xs" onClick={handleAddFileCancel}>
            Cancel
          </Button>
        </div>
      )}

      {/* Editor body — bg-card matches the lhp-* Monaco themes */}
      {focused && activeDesigner && (
        <div className="flex min-h-0 flex-1 flex-col bg-card">
          <Suspense fallback={<EditorSkeleton />}>
            <DesignerCanvas
              key={activeDesigner.id}
              pipeline={activeDesigner.pipeline}
              flowgroup={activeDesigner.flowgroup}
              filePath={activeDesigner.filePath}
              docKind={activeDesigner.docKind}
            />
          </Suspense>
        </div>
      )}
      {focused && !activeDesigner && (
        <div className="flex min-h-0 flex-1 flex-col bg-card">
          {activeBuffer && loadFailed && (
            <div className="flex items-center gap-3 border-b border-border bg-muted px-4 py-2 text-xs">
              <span className="text-destructive">
                This file failed to load — editing is disabled so an empty buffer can't
                overwrite it.
              </span>
              <Button
                variant="outline"
                size="xs"
                onClick={() => handleRetryLoad(activeBuffer.path)}
              >
                Retry
              </Button>
            </div>
          )}
          {activeBuffer && !activeBuffer.exists && !activeBuffer.loading && !loadFailed && (
            <div className="border-b border-border bg-muted px-4 py-2 text-xs text-muted-foreground">
              This file doesn't exist yet. Save to create it.
            </div>
          )}
          {activeBuffer && !activeBuffer.loading ? (
            <div className="min-h-0 flex-1">
              <Suspense fallback={<EditorSkeleton />}>
                <MonacoEditorWrapper
                  key={activeBuffer.path}
                  ref={editorRef}
                  path={activeBuffer.path}
                  content={activeBuffer.content}
                  readOnly={isReadOnly || loadFailed}
                  onDirtyChange={handleDirtyChange}
                  onSave={saveActive}
                  onEditorMount={handleEditorMount}
                />
              </Suspense>
            </div>
          ) : (
            <EditorSkeleton />
          )}
        </div>
      )}

      {/* Dirty-close confirmation */}
      <DiscardChangesDialog
        open={pendingClose !== null}
        onOpenChange={(open) => {
          if (!open) setPendingClose(null)
        }}
        description={`Unsaved changes to ${pendingCloseName ?? 'this file'} will be lost.`}
        onDiscard={handleConfirmedClose}
      />

      {/* 412 stale-write resolution */}
      <ConflictDialog
        conflict={conflict}
        onCancel={cancelConflict}
        onKeepMine={resolveKeepMine}
        onTakeTheirs={resolveTakeTheirs}
      />
    </div>
  )
}
