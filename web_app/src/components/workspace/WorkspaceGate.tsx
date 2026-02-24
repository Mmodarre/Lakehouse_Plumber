import type { ReactNode } from 'react'
import { useUIStore } from '../../store/uiStore'
import { useCreateWorkspace } from '../../hooks/useWorkspace'
import { LoadingSpinner } from '../common/LoadingSpinner'

export function WorkspaceGate({ children }: { children: ReactNode }) {
  const workspaceStatus = useUIStore((s) => s.workspaceStatus)
  const workspaceError = useUIStore((s) => s.workspaceError)
  const setWorkspaceStatus = useUIStore((s) => s.setWorkspaceStatus)
  const createMutation = useCreateWorkspace()

  if (workspaceStatus === 'active') {
    return <>{children}</>
  }

  if (workspaceStatus === 'init' || workspaceStatus === 'checking') {
    return (
      <div className="flex h-full flex-col items-center justify-center gap-3">
        <LoadingSpinner />
        <p className="text-sm text-slate-500">
          {workspaceStatus === 'init' ? 'Connecting...' : 'Checking workspace...'}
        </p>
      </div>
    )
  }

  if (workspaceStatus === 'creating') {
    return (
      <div className="flex h-full flex-col items-center justify-center gap-3">
        <LoadingSpinner />
        <p className="text-sm text-slate-500">Setting up workspace...</p>
        <p className="text-xs text-slate-400">This may take a moment while the repository is cloned</p>
      </div>
    )
  }

  if (workspaceStatus === 'error') {
    return (
      <div className="flex h-full flex-col items-center justify-center gap-4">
        <div className="flex h-12 w-12 items-center justify-center rounded-full bg-red-50">
          <svg className="h-6 w-6 text-red-400" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-2.5L13.732 4c-.77-.833-1.964-.833-2.732 0L4.082 16.5c-.77.833.192 2.5 1.732 2.5z" />
          </svg>
        </div>
        <div className="text-center">
          <p className="text-sm font-medium text-slate-700">Connection Error</p>
          <p className="mt-1 text-xs text-slate-500">
            {workspaceError ?? 'Unable to reach the workspace service'}
          </p>
        </div>
        <button
          onClick={() => setWorkspaceStatus('checking')}
          className="rounded bg-slate-800 px-4 py-1.5 text-xs font-medium text-white hover:bg-slate-700"
        >
          Retry
        </button>
      </div>
    )
  }

  // workspaceStatus === 'no_workspace'
  return (
    <div className="flex h-full flex-col items-center justify-center gap-4">
      <div className="flex h-12 w-12 items-center justify-center rounded-full bg-blue-50">
        <svg className="h-6 w-6 text-blue-500" fill="none" viewBox="0 0 24 24" stroke="currentColor">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M3 7v10a2 2 0 002 2h14a2 2 0 002-2V9a2 2 0 00-2-2h-6l-2-2H5a2 2 0 00-2 2z" />
        </svg>
      </div>
      <div className="text-center">
        <p className="text-sm font-medium text-slate-700">Create Workspace</p>
        <p className="mt-1 max-w-xs text-xs text-slate-500">
          A workspace is your personal copy of the project for editing.
          Create one to start working.
        </p>
      </div>
      <button
        onClick={() => createMutation.mutate()}
        disabled={createMutation.isPending}
        className="rounded bg-blue-600 px-4 py-1.5 text-xs font-medium text-white hover:bg-blue-500 disabled:opacity-50"
      >
        {createMutation.isPending ? 'Creating...' : 'Create Workspace'}
      </button>
    </div>
  )
}
