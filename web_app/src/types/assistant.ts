// Assistant panel contract types.
//
// Non-streaming DTOs are re-exported from the generated OpenAPI schema
// (`./api.generated`, regenerated via `npm run gen:api`) so backend drift
// surfaces as a tsc break. The NDJSON chat-stream frames have no OpenAPI
// counterpart — they mirror EXACTLY the pinned frame vocabulary in the
// backend's `lhp.webapp.services.assistant_chat` module docstring.

import type { components } from './api.generated'
import type { ErrorFrame } from './api'

type Schemas = components['schemas']

// ── Non-streaming DTOs (generated) ───────────────────────

export type AssistantStatus = Schemas['AssistantStatus']
export type ActiveSessionInfo = Schemas['ActiveSessionInfo']
export type ExecutorConfig = Schemas['ExecutorConfig']
export type ExecutorConfigUpdate = Schemas['ExecutorConfigUpdate']
export type ExecutorMode = ExecutorConfig['mode']
export type ExecutorProvider = NonNullable<ExecutorConfig['provider']>
export type DatabricksProfilesResponse = Schemas['DatabricksProfilesResponse']
export type ChatRequest = Schemas['ChatRequest']
/** Per-turn approval policy (claude_sdk provider): mirrors Claude Code's
 * permission modes. Optional in the wire schema; the UI always sends one. */
export type PermissionMode = NonNullable<ChatRequest['permission_mode']>
export type ApprovalRequestBody = Schemas['ApprovalRequest']
export type ApprovalAction = ApprovalRequestBody['action']
export type SessionSnapshot = Schemas['SessionSnapshot']
export type SessionListItem = Schemas['SessionListItem']
export type SessionListResponse = Schemas['SessionListResponse']
export type DaemonStartResponse = Schemas['DaemonStartResponse']
export type SkillInstallResponse = Schemas['SkillInstallResponse']
export type AssistantSuccessResponse = Schemas['SuccessResponse']

// ── Item / approval payload shapes ───────────────────────

/**
 * One conversation item, in the FLAT live-stream shape carried by an
 * `item.done` frame (`{id, type, status, name, arguments, ...}`). Session
 * snapshots envelope the same data (see {@link SnapshotItemEnvelope});
 * both paths normalize to this shape before rendering.
 */
export interface AssistantItem {
  id?: string
  type?: string
  status?: string
  name?: string
  [key: string]: unknown
}

/**
 * Snapshot item envelope (spike S8): `GET /api/assistant/session` items
 * arrive as `{id, type, status, response_id, created_at, created_by,
 * data: {...}}` — the flat item lives under `data` and must be unwrapped
 * (merging `id`/`type`/`status` from the envelope) before rendering.
 */
export interface SnapshotItemEnvelope {
  id?: string
  type?: string
  status?: string
  response_id?: string
  created_at?: string
  created_by?: string
  data?: Record<string, unknown>
  [key: string]: unknown
}

/**
 * Elicitation params carried by an `approval.request` frame. The documented
 * MCP fields are optional (never observed live in the spike — no ask
 * policies were configured); renderers must fall back to raw JSON.
 */
export interface ApprovalParams {
  message?: string
  phase?: string
  policy_name?: string
  content_preview?: string
  [key: string]: unknown
}

// ── NDJSON chat-stream frames (pinned backend vocabulary) ─

export interface TextDeltaFrame {
  type: 'text.delta'
  delta: string
}

export interface ReasoningDeltaFrame {
  type: 'reasoning.delta'
  delta: string
}

export interface ItemDoneFrame {
  type: 'item.done'
  item: AssistantItem
}

export interface ApprovalRequestFrame {
  type: 'approval.request'
  elicitation_id: string
  params: ApprovalParams
}

export interface AssistantStatusFrame {
  type: 'status'
  state: 'preparing' | 'running' | (string & {})
}

/** Emitted once per turn after provisioning; `created: true` mid-conversation
 * means the stale session was silently replaced (render a divider). */
export interface AssistantSessionFrame {
  type: 'session'
  session_id: string
  created: boolean
}

export interface TurnCompletedFrame {
  type: 'turn.completed'
}

export interface TurnFailedFrame {
  type: 'turn.failed'
  reason: string
}

export interface InterruptedFrame {
  type: 'interrupted'
}

export type SessionFailedHint =
  | 'omnigent_setup'
  | 'databricks_auth'
  | 'claude_auth'
  | 'claude_setup'
  | 'unknown'

export interface SessionFailedFrame {
  type: 'session.failed'
  detail: string
  hint: SessionFailedHint
}

export interface HeartbeatFrame {
  type: 'heartbeat'
}

// The terminal `error` frame reuses the run-stream `ErrorFrame` shape
// (`{type:'error', code, title, ...}`); a dropped daemon is `LHP-GEN-902`.
export type AssistantErrorFrame = ErrorFrame

export type AssistantFrame =
  | TextDeltaFrame
  | ReasoningDeltaFrame
  | ItemDoneFrame
  | ApprovalRequestFrame
  | AssistantStatusFrame
  | AssistantSessionFrame
  | TurnCompletedFrame
  | TurnFailedFrame
  | InterruptedFrame
  | SessionFailedFrame
  | HeartbeatFrame
  | AssistantErrorFrame
