import { create } from 'zustand'
import { persist } from 'zustand/middleware'
import { ApiError } from '../api/client'
import type { ErrorFrame } from '../types/api'
import type {
  ApprovalAction,
  ApprovalParams,
  AssistantFrame,
  AssistantItem,
  SessionFailedHint,
  SessionSnapshot,
  SnapshotItemEnvelope,
} from '../types/assistant'

// ── assistantStore — single source of truth for the chat panel ──
//
// The transport hook (`useAssistantStream`) only decodes frames; this
// store owns their *meaning*: the ordered message parts, the streaming
// flag, the pending approval, session identity, and every failure state.
// All frame→state transitions live in `applyFrame` so the thread, the
// composer, and the failure cards read from exactly one reducer.
//
// Only `panelOpen` is persisted — a reloaded page starts with an empty
// thread and rehydrates from `GET /api/assistant/session`.

/** One ordered slice of the conversation. Deltas coalesce into the LAST
 * part when it matches (assistant text into an open text part, reasoning
 * into an open reasoning part); any other part kind arriving in between
 * starts a new part, preserving interleaving order. */
export type MessagePart =
  | { id: number; kind: 'text'; role: 'user' | 'assistant'; text: string }
  | { id: number; kind: 'reasoning'; text: string }
  | { id: number; kind: 'item'; item: AssistantItem }
  | {
      id: number
      kind: 'approval'
      elicitationId: string
      params: ApprovalParams
      resolved: ApprovalAction | null
    }
  | { id: number; kind: 'divider'; label: string }

/** Every in-panel failure state (never toast-only). */
export type AssistantFailure =
  | { kind: 'session_failed'; detail: string; hint: SessionFailedHint }
  | { kind: 'turn_failed'; reason: string }
  /** Terminal `error` frame (e.g. `LHP-GEN-902` daemon connection lost). */
  | { kind: 'stream_error'; frame: ErrorFrame }
  /** Chat-gate 409 (`LHP-WEB-001/002/003`) surfaced on stream open. */
  | { kind: 'gate'; code: string; message: string }
  | { kind: 'transport'; message: string }

export interface PendingApproval {
  elicitationId: string
  params: ApprovalParams
}

export interface SessionMeta {
  sessionId: string
}

interface AssistantState {
  /** Ordered conversation parts (user + assistant, all kinds). */
  parts: MessagePart[]
  /** True from send until the turn's stream closes or fails. */
  streaming: boolean
  /** Latest `status` frame state ('preparing' | 'running'), null when idle. */
  statusState: string | null
  /** The unresolved elicitation, if the turn is blocked on one. */
  pendingApproval: PendingApproval | null
  /** Identity of the provisioned omnigent session, once known. */
  sessionMeta: SessionMeta | null
  /** Set on any failure frame / transport error; cleared on the next send. */
  failure: AssistantFailure | null
  /** True once an `interrupted` frame arrived for the current turn. */
  interrupted: boolean
  /** Panel visibility — the ONLY persisted field. */
  panelOpen: boolean

  // Actions
  setPanelOpen: (open: boolean) => void
  togglePanel: () => void
  /** Append the user message and mark the turn streaming. */
  beginTurn: (text: string) => void
  /** Fold one decoded NDJSON frame into panel state. */
  applyFrame: (frame: AssistantFrame) => void
  /** Record a stream-open/transport failure (no error frame arrived). */
  failTransport: (error: Error) => void
  /** Mark the stream closed (clean end, terminal frame, or abort). */
  finishStream: (info: { aborted: boolean }) => void
  /** Mark an approval part resolved after the mutation succeeds. */
  resolveApprovalLocal: (elicitationId: string, action: ApprovalAction) => void
  /** Replace the (empty) thread with a session snapshot's items. */
  hydrateFromSnapshot: (snapshot: SessionSnapshot) => void
  /** Clear the thread for a fresh session (POST /session/new succeeded). */
  newConversation: () => void
}

/** Omit that distributes over a union (plain `Omit` collapses it). */
type DistributiveOmit<T, K extends PropertyKey> = T extends unknown
  ? Omit<T, K>
  : never

let nextPartId = 1
function part(p: DistributiveOmit<MessagePart, 'id'>): MessagePart {
  return { id: nextPartId++, ...p } as MessagePart
}

/**
 * Unwrap one snapshot item envelope (spike S8) into the flat live-stream
 * item shape: `data` is the item body; the envelope's `id`/`type`/`status`
 * win over any same-named keys inside `data`.
 */
export function normalizeSnapshotItem(raw: SnapshotItemEnvelope): AssistantItem {
  const data =
    raw.data !== null && typeof raw.data === 'object' ? raw.data : {}
  const merged: AssistantItem = { ...data }
  if (raw.id !== undefined) merged.id = raw.id
  if (raw.type !== undefined) merged.type = raw.type
  if (raw.status !== undefined) merged.status = raw.status
  return merged
}

/** Tolerant text extraction from a message item's `content` — a plain
 * string, or an array of `{text}` entries (input_text / output_text). */
function extractItemText(content: unknown): string {
  if (typeof content === 'string') return content
  if (!Array.isArray(content)) return ''
  return content
    .map((entry) =>
      typeof entry === 'object' && entry !== null && 'text' in entry
        ? String((entry as { text: unknown }).text ?? '')
        : '',
    )
    .join('')
}

/** Map normalized snapshot items onto message parts (the same shapes the
 * live stream produces), so one renderer path serves both. */
function partsFromSnapshot(items: SnapshotItemEnvelope[]): MessagePart[] {
  const parts: MessagePart[] = []
  for (const raw of items) {
    const item = normalizeSnapshotItem(raw)
    if (item.type === 'message') {
      const role = item.role === 'user' ? 'user' : 'assistant'
      const text = extractItemText(item.content)
      if (text !== '') parts.push(part({ kind: 'text', role, text }))
    } else if (item.type === 'reasoning') {
      const text =
        extractItemText(item.summary) || extractItemText(item.content)
      if (text !== '') parts.push(part({ kind: 'reasoning', text }))
    } else {
      parts.push(part({ kind: 'item', item }))
    }
  }
  return parts
}

/** Append a delta to the last part when it matches, else open a new part. */
function coalesceDelta(
  parts: MessagePart[],
  kind: 'text' | 'reasoning',
  delta: string,
): MessagePart[] {
  const last = parts[parts.length - 1]
  if (kind === 'text') {
    if (last !== undefined && last.kind === 'text' && last.role === 'assistant') {
      return [...parts.slice(0, -1), { ...last, text: last.text + delta }]
    }
    return [...parts, part({ kind: 'text', role: 'assistant', text: delta })]
  }
  if (last !== undefined && last.kind === 'reasoning') {
    return [...parts.slice(0, -1), { ...last, text: last.text + delta }]
  }
  return [...parts, part({ kind: 'reasoning', text: delta })]
}

const conversationInitial = {
  parts: [] as MessagePart[],
  streaming: false,
  statusState: null as string | null,
  pendingApproval: null as PendingApproval | null,
  sessionMeta: null as SessionMeta | null,
  failure: null as AssistantFailure | null,
  interrupted: false,
}

export const useAssistantStore = create<AssistantState>()(
  persist(
    (set) => ({
      ...conversationInitial,
      panelOpen: false,

      setPanelOpen: (open) => set({ panelOpen: open }),
      togglePanel: () => set((s) => ({ panelOpen: !s.panelOpen })),

      beginTurn: (text) =>
        set((s) => ({
          parts: [...s.parts, part({ kind: 'text', role: 'user', text })],
          streaming: true,
          statusState: null,
          failure: null,
          interrupted: false,
          // A pending elicitation belongs to the previous turn; it cannot
          // be resolved once a new turn starts.
          pendingApproval: null,
        })),

      applyFrame: (frame) =>
        set((s) => {
          switch (frame.type) {
            case 'text.delta':
              return { parts: coalesceDelta(s.parts, 'text', frame.delta) }

            case 'reasoning.delta':
              return { parts: coalesceDelta(s.parts, 'reasoning', frame.delta) }

            case 'item.done':
              return { parts: [...s.parts, part({ kind: 'item', item: frame.item })] }

            case 'approval.request': {
              const params = frame.params ?? {}
              return {
                pendingApproval: {
                  elicitationId: frame.elicitation_id,
                  params,
                },
                parts: [
                  ...s.parts,
                  part({
                    kind: 'approval',
                    elicitationId: frame.elicitation_id,
                    params,
                    resolved: null,
                  }),
                ],
              }
            }

            case 'status':
              return { statusState: frame.state }

            case 'session': {
              const meta = { sessionId: frame.session_id }
              // A `created: true` arriving mid-conversation means the stale
              // session was silently replaced — surface a subtle divider
              // before this turn's user message.
              if (frame.created && s.parts.length > 1) {
                const parts = [...s.parts]
                const last = parts[parts.length - 1]
                const divider = part({
                  kind: 'divider',
                  label: 'new session started',
                })
                if (last?.kind === 'text' && last.role === 'user') {
                  parts.splice(parts.length - 1, 0, divider)
                } else {
                  parts.push(divider)
                }
                return { sessionMeta: meta, parts }
              }
              return { sessionMeta: meta }
            }

            case 'turn.completed':
              return { streaming: false, statusState: null }

            case 'turn.failed':
              return {
                streaming: false,
                statusState: null,
                failure: { kind: 'turn_failed', reason: frame.reason },
              }

            case 'interrupted':
              return { streaming: false, statusState: null, interrupted: true }

            case 'session.failed':
              return {
                streaming: false,
                statusState: null,
                failure: {
                  kind: 'session_failed',
                  detail: frame.detail,
                  hint: frame.hint,
                },
              }

            case 'error':
              return {
                streaming: false,
                statusState: null,
                failure: { kind: 'stream_error', frame },
              }

            case 'heartbeat':
              return {}

            default:
              return {}
          }
        }),

      failTransport: (error) =>
        set(() => {
          if (error instanceof ApiError && error.code.startsWith('LHP-WEB-')) {
            return {
              streaming: false,
              statusState: null,
              failure: { kind: 'gate', code: error.code, message: error.message },
            }
          }
          return {
            streaming: false,
            statusState: null,
            failure: { kind: 'transport', message: error.message },
          }
        }),

      finishStream: () => set({ streaming: false, statusState: null }),

      resolveApprovalLocal: (elicitationId, action) =>
        set((s) => ({
          pendingApproval:
            s.pendingApproval?.elicitationId === elicitationId
              ? null
              : s.pendingApproval,
          parts: s.parts.map((p) =>
            p.kind === 'approval' && p.elicitationId === elicitationId
              ? { ...p, resolved: action }
              : p,
          ),
        })),

      hydrateFromSnapshot: (snapshot) =>
        set({
          parts: partsFromSnapshot(snapshot.items as SnapshotItemEnvelope[]),
          sessionMeta: { sessionId: snapshot.session_id },
        }),

      newConversation: () => set({ ...conversationInitial }),
    }),
    {
      name: 'lhp-assistant',
      // Conversation state is transient by design — a reload rehydrates
      // from the session snapshot. Only the panel toggle survives.
      partialize: (s) => ({ panelOpen: s.panelOpen }),
    },
  ),
)
