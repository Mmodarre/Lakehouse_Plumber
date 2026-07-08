// ── Push channel (GET /api/events, SSE) ──────────────────
//
// Payload shapes for the two named server-push events. The server sends
// each as an SSE `event:` field plus a single-line JSON `data:` body.

/** `file-changed` payload: project-root-relative paths, batched. */
export interface FileChangedPayload {
  paths: string[]
}

export type RunKind = 'validate' | 'generate'
export type RunStatus = 'running' | 'completed' | 'failed'

/** `run-updated` payload: lifecycle updates for a validate/generate run. */
export interface RunUpdatedPayload {
  run_id: string
  kind: RunKind
  status: RunStatus
}
