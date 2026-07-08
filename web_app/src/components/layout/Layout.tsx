import { useEffect } from 'react'
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
import { cn } from '../../lib/utils'

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
        {/* Reserved right rail (~360px) for the assistant panel (P5). Kept
            collapsed until the panel lands — mount it here by dropping the
            `hidden` class; the surrounding grid already accommodates it. */}
        <aside
          aria-hidden="true"
          className="hidden w-[360px] shrink-0 border-l border-border bg-sidebar"
        />
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
