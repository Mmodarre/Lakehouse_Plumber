import { lazy, Suspense, useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { toast } from 'sonner'
import { Network } from 'lucide-react'
import {
  isReadOnlyPath,
  tabBufferPath,
  useWorkspaceStore,
  workspaceTabId,
  type WorkspaceTabRef,
} from '../../../store/workspaceStore'
import { useRunStore } from '../../../store/runStore'
import { useDocumentStore } from '../../../store/documentStore'
import { useDirtyGuardSource } from '../../../store/dirtyGuardStore'
import { useBeforeUnloadGuard } from '../../../hooks/useBeforeUnloadGuard'
import { EmptyState } from '../../common/EmptyState'
import { DiscardChangesDialog } from '../../editor/DiscardChangesDialog'
import { ConflictDialog } from '../../editor/ConflictDialog'
import { isYamlPath } from '../../editor/yamlSaveSupport'
import { loadBufferContent } from '../../workspace/flowgroupBuffers'
import { useWorkspaceSave } from '../../workspace/useWorkspaceSave'
import type { MonacoEditorHandle } from '../../editor/MonacoEditorWrapper'
import { TabStrip } from './TabStrip'
import { EntityHeader } from './EntityHeader'
import { ConfigHeader } from './ConfigHeader'
import { YamlView } from './YamlView'
import { TableDetailView } from './TableDetailView'
import { ResourceStub } from './ResourceStub'

// View bodies are lazy so the yaml (Monaco), graph and config-form stacks stay
// out of the eager shell chunk (bundle size gate). CodeView / GraphView /
// ConfigFormView are named exports → mapped to a default for React.lazy.
const ProjectMapView = lazy(() => import('./ProjectMapView'))
const PipelineDagView = lazy(() => import('./PipelineDagView'))
const CodeView = lazy(() =>
  import('./CodeView').then((m) => ({ default: m.CodeView })),
)
const GraphView = lazy(() =>
  import('../../entity/GraphView').then((m) => ({ default: m.GraphView })),
)
const ConfigFormView = lazy(() =>
  import('../../entity/ConfigFormView').then((m) => ({ default: m.ConfigFormView })),
)

// ── CenterArea — the active-tab host (§3 / §6.4) ─────────────
//
// The center region of the unified workspace: a TabStrip over every open
// entity plus the body of the active tab. Recomposes the editor machinery of
// the retired WorkspaceEditor (content capture, ⌘S save via useWorkspaceSave,
// 412 conflict + yaml_error wiring, the one-time restore prompt, the dirty
// guard) around a SINGLE Monaco (in YamlView) keyed by the active buffer path.
// Other tab kinds route to the designer canvas (graph view), the config Form
// view, the project map, or the table-detail / resource stubs.

const CAPTURE_DEBOUNCE_MS = 500

function CenterSkeleton() {
  return (
    <div className="flex h-full items-center justify-center bg-card">
      <div className="h-6 w-6 animate-spin rounded-full border-2 border-border border-t-muted-foreground" />
    </div>
  )
}

/** The buffer path a tab edits through Monaco RIGHT NOW (view-aware), or null
 * when the active tab renders a non-Monaco body (graph/map/table/resource).
 * For an entity tab this is its source yaml when the Code view is active (the
 * Code view hosts the editable yaml sub-tab); for a config tab, when YAML is
 * active. Distinct from {@link tabBufferPath}, which is view-agnostic (used for
 * the dirty dot and close target). */
function yamlBufferPathFor(tab: WorkspaceTabRef): string | null {
  switch (tab.kind) {
    case 'file':
      return tab.path
    case 'entity':
      return tab.view === 'code' ? tab.filePath : null
    case 'config':
      return tab.view === 'yaml' ? tab.path : null
    default:
      return null
  }
}

export function CenterArea() {
  const tabs = useWorkspaceStore((s) => s.tabs)
  const activePath = useWorkspaceStore((s) => s.activePath)
  const buffers = useWorkspaceStore((s) => s.buffers)
  const setActive = useWorkspaceStore((s) => s.setActive)
  const closeTab = useWorkspaceStore((s) => s.closeTab)
  const openProjectMap = useWorkspaceStore((s) => s.openProjectMap)
  const updateContent = useWorkspaceStore((s) => s.updateContent)
  const setDirty = useWorkspaceStore((s) => s.setDirty)
  const discardDirty = useWorkspaceStore((s) => s.discardDirty)
  const restoredDirtyCount = useWorkspaceStore((s) => s.restoredDirtyCount)
  const ackRestore = useWorkspaceStore((s) => s.ackRestore)

  const editorRef = useRef<MonacoEditorHandle>(null)
  // Mirrors the currently-mounted BUFFER path (null when the active tab shows a
  // non-Monaco body) so async completions never target the wrong buffer.
  const activePathRef = useRef<string | null>(null)
  const captureTimerRef = useRef<number | null>(null)

  const [pendingClose, setPendingClose] = useState<string | null>(null)

  const activeTab = useMemo(
    () => tabs.find((t) => workspaceTabId(t) === activePath) ?? null,
    [tabs, activePath],
  )
  const activeBufferPath = activeTab ? yamlBufferPathFor(activeTab) : null

  // The buffer path the active tab's BODY reads, regardless of view: file tabs,
  // and entity/config tabs in ANY view (Form/Graph read the buffer through
  // documentStore; YAML through Monaco). Distinct from `activeBufferPath`
  // (view-aware — non-null only for the Monaco/YAML body): a Form/Graph body
  // still needs the buffer content loaded, so the fetch-on-open effect keys on
  // THIS. Resource/map/table tabs have no buffer body → null.
  const activeBodyBufferPath = useMemo(() => {
    if (!activeTab) return null
    switch (activeTab.kind) {
      case 'file':
        return activeTab.path
      case 'entity':
        return activeTab.filePath
      case 'config':
        return activeTab.path
      default:
        return null
    }
  }, [activeTab])

  const anyDirty = buffers.some((b) => b.isDirty)
  const dirtyCount = buffers.filter((b) => b.isDirty).length
  const anySaving = buffers.some((b) => b.isSaving)

  useBeforeUnloadGuard(anyDirty)

  useEffect(() => {
    activePathRef.current = activeBufferPath
  }, [activeBufferPath])

  // ── content capture ────────────────────────────────────────

  const cancelCapture = useCallback(() => {
    if (captureTimerRef.current !== null) {
      window.clearTimeout(captureTimerRef.current)
      captureTimerRef.current = null
    }
  }, [])

  const captureActive = useCallback(() => {
    cancelCapture()
    const path = activePathRef.current
    if (path && editorRef.current) {
      const text = editorRef.current.getValue()
      updateContent(path, text)
      // Keep the entity document's parse handle synced with YAML-view edits
      // (Form/Graph aren't mounted here to do it themselves). Only for paths
      // documentStore has open, and echo-suppressed when the text is unchanged.
      if (useDocumentStore.getState().docs[path]) {
        useDocumentStore.getState().reparse(path, text)
      }
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

  // Flush the live editor into the store right before unload (sync writes).
  useEffect(() => {
    window.addEventListener('beforeunload', captureActive)
    return () => window.removeEventListener('beforeunload', captureActive)
  }, [captureActive])

  const handleDirtyChange = useCallback(
    (dirty: boolean) => {
      const path = activePathRef.current
      if (!path) return
      if (dirty) {
        setDirty(path, true)
        scheduleCapture()
      } else {
        cancelCapture()
        setDirty(path, false)
      }
    },
    [setDirty, scheduleCapture, cancelCapture],
  )

  // Flush the OUTGOING Monaco buffer when the shown YAML buffer changes.
  // Zustand subscribers run synchronously inside set(), BEFORE React
  // re-renders/unmounts YamlView, so editorRef still holds the outgoing editor.
  //
  // Keying on the *shown YAML buffer path* (not activePath) is load-bearing:
  // a same-tab YAML→Form/Graph switch keeps activePath fixed but changes the
  // shown buffer to null. Without this flush, text typed <500ms before the
  // switch (its debounced capture never fired) is lost, and a subsequent Form
  // `mutate` would serialize a stale handle over it (the T2.1 lost-update bug).
  // The reparse keeps the entity document's handle in sync with the just-typed
  // text so the Form/Graph view mounts on current content.
  useEffect(() => {
    const activeYamlPath = (
      s: ReturnType<typeof useWorkspaceStore.getState>,
    ): string | null => {
      const tab = s.tabs.find((t) => workspaceTabId(t) === s.activePath)
      return tab ? yamlBufferPathFor(tab) : null
    }
    let prev = activeYamlPath(useWorkspaceStore.getState())
    return useWorkspaceStore.subscribe((s) => {
      const next = activeYamlPath(s)
      if (next === prev) return
      const outPath = prev
      prev = next
      if (
        outPath !== null &&
        editorRef.current &&
        activePathRef.current === outPath &&
        s.buffers.some((b) => b.path === outPath)
      ) {
        cancelCapture()
        const text = editorRef.current.getValue()
        updateContent(outPath, text)
        if (useDocumentStore.getState().docs[outPath]) {
          useDocumentStore.getState().reparse(outPath, text)
        }
      }
    })
  }, [updateContent, cancelCapture])

  // Drain buffers restored from localStorage as `loading` placeholders (clean
  // buffers persist without content — see workspaceStore.partialize).
  useEffect(() => {
    for (const b of useWorkspaceStore.getState().buffers) {
      if (b.loading) void loadBufferContent(b.path)
    }
  }, [])

  // Ensure the active buffer-backed tab has a buffer: entity/config tabs opened
  // by the explorer (openEntityTab / openConfigTab) carry NO buffer, and a
  // Form/Graph body reads the buffer through documentStore — without this a
  // freshly-opened Form tab (the default view) would skeleton forever. Keyed on
  // the view-agnostic body path so it also covers Form/Graph, not just YAML.
  // Reuse the shared loader (never a second one).
  useEffect(() => {
    if (!activeBodyBufferPath) return
    if (!useWorkspaceStore.getState().buffers.some((b) => b.path === activeBodyBufferPath)) {
      useWorkspaceStore.getState().openBuffer(activeBodyBufferPath, {
        loading: true,
        activate: false,
      })
      void loadBufferContent(activeBodyBufferPath)
    }
  }, [activeBodyBufferPath])

  // ── save pipeline ──────────────────────────────────────────

  const { saveActive, saveAllDirty, conflict, cancelConflict, resolveKeepMine, resolveTakeTheirs } =
    useWorkspaceSave({ editorRef, activePathRef, captureActive })

  // Re-derive YAML syntax markers from the synthetic Problems issue on mount
  // (models are disposed on tab switch, so squiggles would otherwise vanish).
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

  const handleRetryLoad = useCallback((path: string) => {
    void loadBufferContent(path).then(() => {
      const buf = useWorkspaceStore.getState().buffers.find((b) => b.path === path)
      if (buf && !buf.loadFailed && activePathRef.current === path) {
        editorRef.current?.setValue(buf.content)
      }
    })
  }, [])

  // ── restore prompt (one-time, non-blocking) ────────────────

  const applyDiscard = useCallback(() => {
    cancelCapture()
    discardDirty()
    const path = activePathRef.current
    if (!path || !editorRef.current) return
    const active = useWorkspaceStore.getState().buffers.find((b) => b.path === path)
    if (active) editorRef.current.setValue(active.content)
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

  // Dirty state feeds the shared NavigationGuard registry.
  useDirtyGuardSource(
    'workspace',
    useMemo(
      () =>
        anyDirty
          ? {
              message: `Unsaved changes in ${dirtyCount} file(s) will be lost.`,
              onDiscard: applyDiscard,
            }
          : null,
      [anyDirty, dirtyCount, applyDiscard],
    ),
  )

  // ── tab handlers ───────────────────────────────────────────

  const handleSelect = useCallback(
    (id: string) => {
      // The store-subscribe flush (above) captures the outgoing buffer inside
      // this synchronous set() before YamlView re-keys/unmounts.
      setActive(id)
    },
    [setActive],
  )

  /** Remove a tab (and any backing buffer) regardless of kind by delegating to
   * the store's closeTab (which owns the neighbour-focus + buffer-drop
   * mutation, uniform across kinds). cancelCapture and the stale-content toast
   * dismissal stay here because they are UI concerns. */
  const removeTab = useCallback(
    (id: string) => {
      cancelCapture()
      const s = useWorkspaceStore.getState()
      const tab = s.tabs.find((t) => workspaceTabId(t) === id)
      const bp = tab ? tabBufferPath(tab) : null
      if (bp) {
        toast.dismiss(`stale:${bp}`)
        // Drop the entity document's parse handle so handles don't accumulate as
        // tabs close (documentStore is layered on the buffer; closeTab drops the
        // buffer below, this drops the handle). No-op when nothing is open at bp.
        useDocumentStore.getState().close(bp)
      }
      closeTab(id)
    },
    [cancelCapture, closeTab],
  )

  const handleClose = useCallback((id: string) => {
    const s = useWorkspaceStore.getState()
    const tab = s.tabs.find((t) => workspaceTabId(t) === id)
    if (!tab) return
    const bp = tabBufferPath(tab)
    const buf = bp ? s.buffers.find((b) => b.path === bp) : null
    if (buf?.isDirty) {
      setPendingClose(id)
      return
    }
    removeTab(id)
  }, [removeTab])

  const handleConfirmedClose = useCallback(() => {
    if (pendingClose === null) return
    removeTab(pendingClose)
    setPendingClose(null)
  }, [pendingClose, removeTab])

  const pendingCloseName = pendingClose?.split('/').pop() ?? pendingClose

  // ── body routing ───────────────────────────────────────────

  const renderYaml = (bufferPath: string) => {
    const buf = buffers.find((b) => b.path === bufferPath)
    if (!buf || buf.loading) return <CenterSkeleton />
    return (
      <YamlView
        buffer={buf}
        editorRef={editorRef}
        isReadOnly={isReadOnlyPath(buf.path)}
        dirtyCount={dirtyCount}
        anySaving={anySaving}
        onDirtyChange={handleDirtyChange}
        onSave={saveActive}
        onSaveAll={saveAllDirty}
        onEditorMount={handleEditorMount}
        onRetryLoad={handleRetryLoad}
      />
    )
  }

  const renderBody = () => {
    if (!activeTab) {
      return (
        <div className="flex h-full items-center justify-center p-6">
          <EmptyState
            icon={Network}
            title="No entity open"
            message="Select a flowgroup, table or config in the explorer — or open the project map."
            action={{ label: 'Open project map', onClick: () => openProjectMap() }}
          />
        </div>
      )
    }

    switch (activeTab.kind) {
      case 'file':
        return renderYaml(activeTab.path)
      case 'entity': {
        // Keyed by filePath (stable across a flowgroup rename → no remount);
        // tabId is passed as a prop so the view tracks the tab's current id.
        const entityTabId = workspaceTabId(activeTab)
        if (activeTab.view === 'code')
          return (
            <Suspense fallback={<CenterSkeleton />}>
              <CodeView
                key={activeTab.filePath}
                tabId={entityTabId}
                pipeline={activeTab.pipeline}
                flowgroup={activeTab.flowgroup}
                filePath={activeTab.filePath}
              />
            </Suspense>
          )
        return (
          <Suspense fallback={<CenterSkeleton />}>
            <GraphView
              key={activeTab.filePath}
              tabId={entityTabId}
              filePath={activeTab.filePath}
              docKind={activeTab.docKind}
            />
          </Suspense>
        )
      }
      case 'designer':
        // Retired pre-redesign shell tab (no live path creates one; the persist
        // migrate converts stored DesignerTabs → EntityTab). Wave-4-reserved:
        // a future FlowgroupMiniGraph could open one, but it isn't mounted in the
        // live shell yet, so this branch is not currently reachable. Kept (not
        // retired) and rendered on GraphView so the designer canvas layer stays
        // deleted.
        return (
          <Suspense fallback={<CenterSkeleton />}>
            <GraphView
              key={activeTab.filePath}
              tabId={activeTab.id}
              filePath={activeTab.filePath}
              docKind={activeTab.docKind ?? 'flowgroup'}
            />
          </Suspense>
        )
      case 'config':
        if (activeTab.view === 'yaml') return renderYaml(activeTab.path)
        return (
          <Suspense fallback={<CenterSkeleton />}>
            <ConfigFormView
              key={activeTab.path}
              tabId={workspaceTabId(activeTab)}
              path={activeTab.path}
              configKind={activeTab.configKind}
            />
          </Suspense>
        )
      case 'project-map':
        return (
          <Suspense fallback={<CenterSkeleton />}>
            <ProjectMapView />
          </Suspense>
        )
      case 'pipeline-dag':
        return (
          <Suspense fallback={<CenterSkeleton />}>
            <PipelineDagView pipeline={activeTab.pipeline} />
          </Suspense>
        )
      case 'table-detail':
        return <TableDetailView fqn={activeTab.fqn} />
      case 'resource':
        return <ResourceStub resourceKind={activeTab.resourceKind} name={activeTab.name} />
    }
  }

  return (
    <div className="flex h-full min-h-0 min-w-0 flex-col bg-background">
      {tabs.length > 0 && (
        <div className="flex items-center border-b border-border bg-sidebar">
          <TabStrip onSelect={handleSelect} onClose={handleClose} />
        </div>
      )}
      {activeTab?.kind === 'entity' && <EntityHeader tab={activeTab} />}
      {activeTab?.kind === 'config' && <ConfigHeader tab={activeTab} />}
      <div className="flex min-h-0 flex-1 flex-col overflow-hidden">{renderBody()}</div>

      <DiscardChangesDialog
        open={pendingClose !== null}
        onOpenChange={(open) => {
          if (!open) setPendingClose(null)
        }}
        description={`Unsaved changes to ${pendingCloseName ?? 'this file'} will be lost.`}
        onDiscard={handleConfirmedClose}
      />

      <ConflictDialog
        conflict={conflict}
        onCancel={cancelConflict}
        onKeepMine={resolveKeepMine}
        onTakeTheirs={resolveTakeTheirs}
      />
    </div>
  )
}
