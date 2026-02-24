import { Outlet } from 'react-router-dom'
import { Toaster } from 'sonner'
import { Header } from './Header'
import { Sidebar } from './Sidebar'
import { ChatPanel } from '../chat/ChatPanel'
import { DetailModal } from '../detail/DetailModal'
import { FileEditorModal } from '../editor/FileEditorModal'
import { FlowgroupEditorModal } from '../editor/FlowgroupEditorModal'
import { CreateFlowgroupDialog } from '../editor/CreateFlowgroupDialog'
import { ErrorBoundary } from '../common/ErrorBoundary'
import { LoadingSpinner } from '../common/LoadingSpinner'
import { WorkspaceGate } from '../workspace/WorkspaceGate'
import { useHealth } from '../../hooks/useProject'
import { useAIStatus } from '../../hooks/useAIStatus'
import { useWorkspaceCheck, useWorkspaceHeartbeat } from '../../hooks/useWorkspace'
import { useUIStore } from '../../store/uiStore'

export function Layout() {
  const { isLoading: healthLoading, data: health } = useHealth()
  const workspaceStatus = useUIStore((s) => s.workspaceStatus)

  // Initialize workspace lifecycle (no-op in dev mode)
  useWorkspaceCheck()
  useWorkspaceHeartbeat()

  // Poll AI assistant availability (no-op if AI disabled on backend)
  useAIStatus()

  const isDevMode = health?.dev_mode ?? true
  const workspaceReady = isDevMode || workspaceStatus === 'active'

  // Wait for health endpoint before rendering anything
  if (healthLoading) {
    return (
      <div className="flex h-screen flex-col items-center justify-center bg-slate-50">
        <LoadingSpinner />
        <p className="mt-3 text-sm text-slate-500">Connecting...</p>
      </div>
    )
  }

  // Dev mode: render everything directly (no gate)
  if (isDevMode) {
    return (
      <div className="flex h-screen flex-col bg-slate-50">
        <Header />
        <div className="flex min-h-0 flex-1">
          <ErrorBoundary>
            <Sidebar />
          </ErrorBoundary>
          <main className="min-w-0 flex-1">
            <ErrorBoundary>
              <Outlet />
            </ErrorBoundary>
          </main>
          <ErrorBoundary>
            <ChatPanel />
          </ErrorBoundary>
        </div>
        <DetailModal />
        <FileEditorModal />
        <FlowgroupEditorModal />
        <CreateFlowgroupDialog />
        <Toaster position="bottom-right" richColors toastOptions={{ duration: 4000 }} />
      </div>
    )
  }

  // Production mode: gate the main content behind workspace check
  return (
    <div className="flex h-screen flex-col bg-slate-50">
      <Header minimal={!workspaceReady} />
      <WorkspaceGate>
        <div className="flex min-h-0 flex-1">
          <ErrorBoundary>
            <Sidebar />
          </ErrorBoundary>
          <main className="min-w-0 flex-1">
            <ErrorBoundary>
              <Outlet />
            </ErrorBoundary>
          </main>
          <ErrorBoundary>
            <ChatPanel />
          </ErrorBoundary>
        </div>
        <DetailModal />
        <FileEditorModal />
        <FlowgroupEditorModal />
        <CreateFlowgroupDialog />
      </WorkspaceGate>
      <Toaster position="bottom-right" richColors toastOptions={{ duration: 4000 }} />
    </div>
  )
}
