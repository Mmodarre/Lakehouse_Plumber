import { Outlet } from 'react-router-dom'
import { Toaster } from 'sonner'
import { Header } from './Header'
import { Sidebar } from './Sidebar'
import { DetailModal } from '../detail/DetailModal'
import { FileEditorModal } from '../editor/FileEditorModal'
import { FlowgroupEditorModal } from '../editor/FlowgroupEditorModal'
import { CreateFlowgroupDialog } from '../editor/CreateFlowgroupDialog'
import { ErrorBoundary } from '../common/ErrorBoundary'
import { LoadingSpinner } from '../common/LoadingSpinner'
import { useHealth } from '../../hooks/useProject'

export function Layout() {
  const { isLoading: healthLoading } = useHealth()

  // Wait for health endpoint before rendering anything
  if (healthLoading) {
    return (
      <div className="flex h-screen flex-col items-center justify-center bg-slate-50">
        <LoadingSpinner />
        <p className="mt-3 text-sm text-slate-500">Connecting...</p>
      </div>
    )
  }

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
      </div>
      <DetailModal />
      <FileEditorModal />
      <FlowgroupEditorModal />
      <CreateFlowgroupDialog />
      <Toaster position="bottom-right" richColors toastOptions={{ duration: 4000 }} />
    </div>
  )
}
