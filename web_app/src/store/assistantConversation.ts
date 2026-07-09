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
  TurnUsageKeys,
  UsageTotals,
} from '../types/assistant'

// ── assistantConversation — pure per-conversation reducer ──
//
// Every frame→state transition for ONE conversation lives here as pure
// functions over `ConversationState`: no Zustand, no I/O, no module-level
// mutable state. `assistantStore` wraps these into store actions; keeping
// the logic self-contained lets a future store hold multiple live
// conversations side by side.

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

export interface ConversationState {
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
  /** Lifetime token/cost totals for this session, or null before any turn
   * reported usage (omnigent conversations always stay null). */
  usage: UsageTotals | null
  /** Next part id to mint — per-conversation, so ids stay deterministic and
   * collision-free when multiple conversations coexist. */
  nextPartId: number
}

export function emptyConversation(): ConversationState {
  return {
    parts: [],
    streaming: false,
    statusState: null,
    pendingApproval: null,
    sessionMeta: null,
    failure: null,
    interrupted: false,
    usage: null,
    nextPartId: 1,
  }
}

/** Omit that distributes over a union (plain `Omit` collapses it). */
type DistributiveOmit<T, K extends PropertyKey> = T extends unknown
  ? Omit<T, K>
  : never

type PartInput = DistributiveOmit<MessagePart, 'id'>

function mintPart(id: number, p: PartInput): MessagePart {
  return { id, ...p } as MessagePart
}

function appendPart(state: ConversationState, p: PartInput): ConversationState {
  return {
    ...state,
    parts: [...state.parts, mintPart(state.nextPartId, p)],
    nextPartId: state.nextPartId + 1,
  }
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

/** Flip still-`running` tool parts to `incomplete` when the turn ends —
 * the backend never persists started-only items, so a running part whose
 * turn is over will never be reconciled by an `item.done`. */
function markRunningPartsIncomplete(parts: MessagePart[]): MessagePart[] {
  if (!parts.some((p) => p.kind === 'item' && p.item.status === 'running')) {
    return parts
  }
  return parts.map((p) =>
    p.kind === 'item' && p.item.status === 'running'
      ? { ...p, item: { ...p.item, status: 'incomplete' } }
      : p,
  )
}

/** Fold a terminal frame's usage keys into the running session totals.
 * `session_totals` (authoritative: post-insert lifetime sums including
 * history) wins outright; otherwise the frame's own per-turn `usage` /
 * costs accumulate into the previous totals. Bare terminals (omnigent)
 * leave the totals untouched. */
function foldUsage(
  prev: UsageTotals | null,
  frame: TurnUsageKeys,
): UsageTotals | null {
  if (frame.session_totals) return frame.session_totals
  if (!frame.usage) return prev
  const addCost = (base: number | null | undefined, turn: number | undefined) =>
    turn !== undefined ? (base ?? 0) + turn : (base ?? null)
  return {
    input_tokens: (prev?.input_tokens ?? 0) + (frame.usage.input_tokens ?? 0),
    output_tokens: (prev?.output_tokens ?? 0) + (frame.usage.output_tokens ?? 0),
    cache_read_input_tokens:
      (prev?.cache_read_input_tokens ?? 0) +
      (frame.usage.cache_read_input_tokens ?? 0),
    cache_creation_input_tokens:
      (prev?.cache_creation_input_tokens ?? 0) +
      (frame.usage.cache_creation_input_tokens ?? 0),
    sdk_cost_usd: addCost(prev?.sdk_cost_usd, frame.total_cost_usd),
    configured_cost_usd: addCost(
      prev?.configured_cost_usd,
      frame.configured_cost_usd,
    ),
  }
}

/** Append a delta to the last part when it matches, else open a new part. */
function coalesceDelta(
  state: ConversationState,
  kind: 'text' | 'reasoning',
  delta: string,
): ConversationState {
  const last = state.parts[state.parts.length - 1]
  if (kind === 'text') {
    if (last !== undefined && last.kind === 'text' && last.role === 'assistant') {
      return {
        ...state,
        parts: [...state.parts.slice(0, -1), { ...last, text: last.text + delta }],
      }
    }
    return appendPart(state, { kind: 'text', role: 'assistant', text: delta })
  }
  if (last !== undefined && last.kind === 'reasoning') {
    return {
      ...state,
      parts: [...state.parts.slice(0, -1), { ...last, text: last.text + delta }],
    }
  }
  return appendPart(state, { kind: 'reasoning', text: delta })
}

/** Append the user message and mark the turn streaming. */
export function beginTurnInConversation(
  state: ConversationState,
  text: string,
): ConversationState {
  return {
    ...appendPart(state, { kind: 'text', role: 'user', text }),
    streaming: true,
    statusState: null,
    failure: null,
    interrupted: false,
    // A pending elicitation belongs to the previous turn; it cannot
    // be resolved once a new turn starts.
    pendingApproval: null,
  }
}

/** Fold one decoded NDJSON frame into conversation state. */
export function applyFrameToConversation(
  state: ConversationState,
  frame: AssistantFrame,
): ConversationState {
  switch (frame.type) {
    case 'text.delta':
      return coalesceDelta(state, 'text', frame.delta)

    case 'reasoning.delta':
      return coalesceDelta(state, 'reasoning', frame.delta)

    case 'item.started':
      return appendPart(state, { kind: 'item', item: frame.item })

    case 'item.done': {
      // Reconcile with the `item.started` part of the same id IN PLACE so
      // the call keeps its position. No match (the omnigent provider emits
      // no started frames) appends as before.
      const id = frame.item.id
      const idx =
        id === undefined
          ? -1
          : state.parts.findIndex((p) => p.kind === 'item' && p.item.id === id)
      if (idx === -1) return appendPart(state, { kind: 'item', item: frame.item })
      const parts = [...state.parts]
      parts[idx] = { id: parts[idx].id, kind: 'item', item: frame.item }
      return { ...state, parts }
    }

    case 'approval.request': {
      const params = frame.params ?? {}
      return {
        ...appendPart(state, {
          kind: 'approval',
          elicitationId: frame.elicitation_id,
          params,
          resolved: null,
        }),
        pendingApproval: {
          elicitationId: frame.elicitation_id,
          params,
        },
      }
    }

    case 'status':
      return { ...state, statusState: frame.state }

    case 'session': {
      const sessionMeta = { sessionId: frame.session_id }
      // A `created: true` arriving mid-conversation means the stale
      // session was silently replaced — surface a subtle divider
      // before this turn's user message.
      if (frame.created && state.parts.length > 1) {
        const parts = [...state.parts]
        const last = parts[parts.length - 1]
        const divider = mintPart(state.nextPartId, {
          kind: 'divider',
          label: 'new session started',
        })
        if (last?.kind === 'text' && last.role === 'user') {
          parts.splice(parts.length - 1, 0, divider)
        } else {
          parts.push(divider)
        }
        return { ...state, sessionMeta, parts, nextPartId: state.nextPartId + 1 }
      }
      return { ...state, sessionMeta }
    }

    case 'turn.completed':
      return {
        ...state,
        parts: markRunningPartsIncomplete(state.parts),
        streaming: false,
        statusState: null,
        usage: foldUsage(state.usage, frame),
      }

    case 'turn.failed':
      return {
        ...state,
        parts: markRunningPartsIncomplete(state.parts),
        streaming: false,
        statusState: null,
        failure: { kind: 'turn_failed', reason: frame.reason },
        usage: foldUsage(state.usage, frame),
      }

    case 'interrupted':
      return {
        ...state,
        parts: markRunningPartsIncomplete(state.parts),
        streaming: false,
        statusState: null,
        interrupted: true,
        usage: foldUsage(state.usage, frame),
      }

    case 'session.failed':
      return {
        ...state,
        parts: markRunningPartsIncomplete(state.parts),
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
        ...state,
        parts: markRunningPartsIncomplete(state.parts),
        streaming: false,
        statusState: null,
        failure: { kind: 'stream_error', frame },
      }

    case 'heartbeat':
      return state

    default:
      return state
  }
}

/** Record a stream-open/transport failure (no error frame arrived). */
export function failTransportInConversation(
  state: ConversationState,
  error: Error,
): ConversationState {
  const parts = markRunningPartsIncomplete(state.parts)
  if (error instanceof ApiError && error.code.startsWith('LHP-WEB-')) {
    return {
      ...state,
      parts,
      streaming: false,
      statusState: null,
      failure: { kind: 'gate', code: error.code, message: error.message },
    }
  }
  return {
    ...state,
    parts,
    streaming: false,
    statusState: null,
    failure: { kind: 'transport', message: error.message },
  }
}

/** Mark the stream closed (clean end, terminal frame, or abort). */
export function finishStreamInConversation(
  state: ConversationState,
): ConversationState {
  return {
    ...state,
    parts: markRunningPartsIncomplete(state.parts),
    streaming: false,
    statusState: null,
  }
}

/** Mark an approval part resolved after the mutation succeeds. */
export function resolveApprovalInConversation(
  state: ConversationState,
  elicitationId: string,
  action: ApprovalAction,
): ConversationState {
  return {
    ...state,
    pendingApproval:
      state.pendingApproval?.elicitationId === elicitationId
        ? null
        : state.pendingApproval,
    parts: state.parts.map((p) =>
      p.kind === 'approval' && p.elicitationId === elicitationId
        ? { ...p, resolved: action }
        : p,
    ),
  }
}

/** Build a fresh conversation from a session snapshot, mapping normalized
 * items onto the same part shapes the live stream produces, so one
 * renderer path serves both. */
export function conversationFromSnapshot(
  snapshot: SessionSnapshot,
): ConversationState {
  let state = emptyConversation()
  for (const raw of snapshot.items as SnapshotItemEnvelope[]) {
    const item = normalizeSnapshotItem(raw)
    if (item.type === 'message') {
      const role = item.role === 'user' ? 'user' : 'assistant'
      const text = extractItemText(item.content)
      if (text !== '') state = appendPart(state, { kind: 'text', role, text })
    } else if (item.type === 'reasoning') {
      const text =
        extractItemText(item.summary) || extractItemText(item.content)
      if (text !== '') state = appendPart(state, { kind: 'reasoning', text })
    } else {
      state = appendPart(state, { kind: 'item', item })
    }
  }
  return {
    ...state,
    sessionMeta: { sessionId: snapshot.session_id },
    usage: snapshot.usage_totals ?? null,
  }
}
