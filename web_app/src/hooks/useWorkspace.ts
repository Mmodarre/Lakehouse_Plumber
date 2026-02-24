import { useEffect } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { getWorkspace, createWorkspace, sendHeartbeat } from '../api/workspace'
import { useHealth } from './useProject'
import { useUIStore } from '../store/uiStore'

/**
 * Initializes workspace lifecycle on mount.
 * - Dev mode: sets 'active' immediately (no workspace needed)
 * - Production mode: checks GET /api/workspace and syncs result to Zustand
 */
export function useWorkspaceCheck() {
  const { data: health } = useHealth()
  const workspaceStatus = useUIStore((s) => s.workspaceStatus)
  const setWorkspaceStatus = useUIStore((s) => s.setWorkspaceStatus)
  const setWorkspaceError = useUIStore((s) => s.setWorkspaceError)
  const setWorkspaceActive = useUIStore((s) => s.setWorkspaceActive)

  const isDevMode = health?.dev_mode

  // In dev mode, skip workspace check entirely
  useEffect(() => {
    if (isDevMode === true && workspaceStatus === 'init') {
      setWorkspaceActive()
    }
  }, [isDevMode, workspaceStatus, setWorkspaceActive])

  // In production mode, transition from init → checking
  useEffect(() => {
    if (isDevMode === false && workspaceStatus === 'init') {
      setWorkspaceStatus('checking')
    }
  }, [isDevMode, workspaceStatus, setWorkspaceStatus])

  // Query fires only when status is 'checking' in production mode
  const query = useQuery({
    queryKey: ['workspace'],
    queryFn: getWorkspace,
    enabled: isDevMode === false && workspaceStatus === 'checking',
    staleTime: Infinity,
    retry: false,
  })

  // Sync query result → Zustand
  useEffect(() => {
    if (workspaceStatus !== 'checking') return

    if (query.isSuccess && query.data) {
      const state = query.data.state
      if (state === 'active') {
        setWorkspaceActive()
      } else if (state === 'idle' || state === 'creating') {
        // idle = dormant after restart, needs resuming via PUT /workspace
        // creating = backend still initializing
        // Both show spinner and auto-resume
        setWorkspaceStatus('creating')
        createWorkspace()
          .then(() => {
            useUIStore.getState().setWorkspaceActive()
          })
          .catch((err: Error) => {
            useUIStore.getState().setWorkspaceError(err.message ?? 'Failed to resume workspace')
          })
      } else {
        // stopped or deleted — treat as no workspace
        useUIStore.getState().setWorkspaceLost()
      }
    }

    if (query.isError) {
      const err = query.error as { status?: number }
      if (err.status === 404 || err.status === 409) {
        useUIStore.getState().setWorkspaceLost()
      } else {
        setWorkspaceError(query.error?.message ?? 'Failed to check workspace')
      }
    }
  }, [workspaceStatus, query.isSuccess, query.isError, query.data, query.error, setWorkspaceActive, setWorkspaceStatus, setWorkspaceError])
}

/**
 * Mutation for creating a workspace via PUT /api/workspace.
 * Invalidates all queries on success so the app fetches fresh data.
 */
export function useCreateWorkspace() {
  const queryClient = useQueryClient()
  const setWorkspaceStatus = useUIStore((s) => s.setWorkspaceStatus)
  const setWorkspaceActive = useUIStore((s) => s.setWorkspaceActive)
  const setWorkspaceError = useUIStore((s) => s.setWorkspaceError)

  return useMutation({
    mutationFn: createWorkspace,
    onMutate: () => {
      setWorkspaceStatus('creating')
    },
    onSuccess: () => {
      setWorkspaceActive()
      queryClient.invalidateQueries()
    },
    onError: (error: Error) => {
      setWorkspaceError(error.message ?? 'Failed to create workspace')
    },
  })
}

/**
 * Sends heartbeat POST every 5 minutes to keep workspace alive.
 * Only active in production mode when workspace status is 'active'.
 * Fire-and-forget — errors are silently ignored.
 */
export function useWorkspaceHeartbeat() {
  const { data: health } = useHealth()
  const workspaceStatus = useUIStore((s) => s.workspaceStatus)

  const isDevMode = health?.dev_mode
  const enabled = isDevMode === false && workspaceStatus === 'active'

  useEffect(() => {
    if (!enabled) return

    const interval = setInterval(() => {
      sendHeartbeat().catch(() => {})
    }, 5 * 60 * 1000)

    return () => clearInterval(interval)
  }, [enabled])
}
