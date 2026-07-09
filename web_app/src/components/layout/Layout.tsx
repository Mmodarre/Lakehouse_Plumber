import { lazy, Suspense, useEffect } from 'react'
import { Outlet } from 'react-router-dom'
import { Toaster } from '../ui/sonner'
import { Header } from './Header'
import { Sidebar } from './Sidebar'
import { StatusBar } from './StatusBar'
import { OfflineBanner } from './OfflineBanner'
import { DetailModal } from '../detail/DetailModal'
import { CreateFlowgroupDialog } from '../editor/CreateFlowgroupDialog'
import { WorkspaceEditor } from '../workspace/WorkspaceEditor'
import { useFlowgroupEditorBridge } from '../workspace/flowgroupBuffers'
import { ErrorBoundary } from '../common/ErrorBoundary'
import { ModalErrorFallback } from '../common/ModalErrorFallback'
import { InitProjectPage } from '../../pages/InitProjectPage'
import { useHealth } from '../../hooks/useProject'
import { usePushChannel } from '../../hooks/usePushChannel'
import { useUIStore } from '../../store/uiStore'
import { useWorkspaceStore } from '../../store/workspaceStore'
import { useAssistantStore } from '../../store/assistantStore'
import { LoadingSpinner } from '../common/LoadingSpinner'
import { cn } from '../../lib/utils'

// Lazy on purpose: the assistant panel pulls in the markdown stack
// (react-markdown + remark-gfm), which must stay out of the eager app
// chunk. Only the toggle + this shell are eager.
const AssistantPanel = lazy(() => import('../assistant/AssistantPanel'))

/** Drag handle on the assistant dock's left edge. Pointer capture keeps the
 * drag alive outside the strip; the store setter clamps the width. */
function AssistantResizeHandle() {
  const setPanelWidth = useAssistantStore((s) => s.setPanelWidth)

  const onPointerDown = (e: React.PointerEvent<HTMLDivElement>) => {
    e.preventDefault()
    const startX = e.clientX
    const startWidth = useAssistantStore.getState().panelWidth
    e.currentTarget.setPointerCapture(e.pointerId)
    const onMove = (ev: PointerEvent) => {
      // Dragging left of the start point widens the right-docked panel.
      setPanelWidth(startWidth + (startX - ev.clientX))
    }
    const target = e.currentTarget
    const onUp = () => {
      target.removeEventListener('pointermove', onMove)
      target.removeEventListener('pointerup', onUp)
    }
    target.addEventListener('pointermove', onMove)
    target.addEventListener('pointerup', onUp)
  }

  return (
    <div
      role="separator"
      aria-orientation="vertical"
      aria-label="Resize assistant panel"
      tabIndex={0}
      onPointerDown={onPointerDown}
      onKeyDown={(e) => {
        if (e.key !== 'ArrowLeft' && e.key !== 'ArrowRight') return
        e.preventDefault()
        const width = useAssistantStore.getState().panelWidth
        setPanelWidth(width + (e.key === 'ArrowLeft' ? 16 : -16))
      }}
      className="w-1 shrink-0 cursor-col-resize touch-none select-none bg-transparent transition-colors hover:bg-primary/40 focus-visible:bg-primary/40 focus-visible:outline-none active:bg-primary/60"
    />
  )
}

export function Layout() {
  const { data: health, isError: healthError, refetch } = useHealth()
  // Server-push channel: turns /api/events into query invalidations.
  // Mounted once here so every routed view stays fresh.
  usePushChannel()
  // Flowgroup open/create requests (drill modals, tables, create dialog)
  // become workspace buffers instead of the retired editor modals.
  useFlowgroupEditorBridge()

  // Modal state + close actions, used both to render the modals and as
  // ErrorBoundary reset keys: closing a crashed modal flips its key, which
  // clears the boundary so the (now-closed) modal renders cleanly.
  const modalOpen = useUIStore((s) => s.modalOpen)
  const closeModal = useUIStore((s) => s.closeModal)
  const createFlowgroupDialog = useUIStore((s) => s.createFlowgroupDialog)
  const closeCreateFlowgroupDialog = useUIStore((s) => s.closeCreateFlowgroupDialog)

  // Narrow boolean selectors on purpose: keystrokes and per-buffer updates
  // inside the workspace never change these values, so typing in the editor
  // cannot re-render the app shell (the P3 idempotent-setter lesson).
  const hasBuffers = useWorkspaceStore((s) => s.buffers.length > 0)
  const editorFocused = useWorkspaceStore((s) => s.activePath !== null)
  const closeAllBuffers = useWorkspaceStore((s) => s.closeAllBuffers)
  const ensureProjectScope = useWorkspaceStore((s) => s.ensureProjectScope)
  const assistantOpen = useAssistantStore((s) => s.panelOpen)
  const assistantWidth = useAssistantStore((s) => s.panelWidth)

  // Persisted buffers are keyed to a project root: a different served
  // project drops the restored workspace instead of leaking it across.
  const projectRoot = health?.root
  useEffect(() => {
    if (projectRoot) ensureProjectScope(projectRoot)
  }, [projectRoot, ensureProjectScope])

  const noProject = health?.project_state === 'no_project'
  const showWorkspace = hasBuffers && !noProject

  return (
    <div className="flex h-screen flex-col bg-background">
      <Header />
      {healthError && <OfflineBanner onRetry={() => void refetch()} />}
      <div className="flex min-h-0 flex-1">
        <ErrorBoundary>
          <Sidebar />
        </ErrorBoundary>
        <main className="flex min-w-0 flex-1 flex-col">
          {noProject ? (
            /* First run: the init wizard fills the routed area (same slot
               sizing as the Outlet below). On success it invalidates all
               queries, health flips out of no_project, and the normal
               shell replaces this branch automatically. */
            <div className="min-h-0 flex-1">
              <ErrorBoundary>
                <InitProjectPage />
              </ErrorBoundary>
            </div>
          ) : (
            <>
              {/* Docked workspace: tab strip persists while buffers are open;
                  the editor body fills the center when a buffer is focused.
                  "Try again" on a crash closes all buffers (clean reset). */}
              {showWorkspace && (
                <ErrorBoundary onReset={closeAllBuffers}>
                  <WorkspaceEditor />
                </ErrorBoundary>
              )}
              {/* Routed page stays mounted (hidden while the editor is
                  focused) so page state like graph viewport survives edits. */}
              <div
                className={cn(
                  'min-h-0 flex-1',
                  showWorkspace && editorFocused && 'hidden',
                )}
              >
                <ErrorBoundary>
                  <Outlet />
                </ErrorBoundary>
              </div>
            </>
          )}
        </main>
        {/* Assistant panel right dock (P5). Mounted only while open, so a
            closed panel costs nothing and closing aborts any open stream
            (the turn continues server-side; rehydration shows it). */}
        {assistantOpen && (
          <>
            <AssistantResizeHandle />
            <aside
              style={{ width: assistantWidth }}
              className="shrink-0 border-l border-border bg-sidebar"
            >
              <ErrorBoundary>
                <Suspense fallback={<LoadingSpinner className="h-full" />}>
                  <AssistantPanel />
                </Suspense>
              </ErrorBoundary>
            </aside>
          </>
        )}
      </div>
      <StatusBar />

      <ErrorBoundary fallback={<ModalErrorFallback onClose={closeModal} />} resetKeys={[modalOpen]}>
        <DetailModal />
      </ErrorBoundary>
      <ErrorBoundary
        fallback={<ModalErrorFallback onClose={closeCreateFlowgroupDialog} />}
        resetKeys={[createFlowgroupDialog]}
      >
        <CreateFlowgroupDialog />
      </ErrorBoundary>

      {/* ui/sonner wrapper syncs its theme to the resolved app theme */}
      <Toaster position="bottom-right" richColors toastOptions={{ duration: 4000 }} />
    </div>
  )
}
