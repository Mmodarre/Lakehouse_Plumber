import { fetchApi } from './client'
import type { GitStatusResponse, CommitResponse, WorkspaceResponse, HeartbeatResponse } from '../types/api'

export function fetchGitStatus(): Promise<GitStatusResponse> {
  return fetchApi('/workspace/git/status')
}

export function commitChanges(message: string): Promise<CommitResponse> {
  return fetchApi('/workspace/commit', {
    method: 'POST',
    body: JSON.stringify({ message }),
  })
}

export function pushBranch(): Promise<void> {
  return fetchApi('/workspace/push', { method: 'POST' })
}

export function pullLatest(): Promise<void> {
  return fetchApi('/workspace/pull', { method: 'POST' })
}

// ── Workspace Lifecycle ─────────────────────────────────

export function getWorkspace(): Promise<WorkspaceResponse> {
  return fetchApi('/workspace')
}

export function createWorkspace(): Promise<WorkspaceResponse> {
  return fetchApi('/workspace', { method: 'PUT' })
}

export function sendHeartbeat(): Promise<HeartbeatResponse> {
  return fetchApi('/workspace/heartbeat', { method: 'POST' })
}
