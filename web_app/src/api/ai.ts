/**
 * API client for AI assistant endpoints.
 *
 * All AI traffic routes through /api/ai/* — the frontend never
 * connects to OpenCode directly.
 */

import { fetchApi } from './client'
import type { AIStatus, SessionMode } from '../types/chat'

// ── Status & config ──────────────────────────────────────────

export interface AIConfigSummary {
  provider: string
  model: string
  allowed_models: Record<string, string[]>
}

export function fetchAIStatus(): Promise<AIStatus> {
  return fetchApi<AIStatus>('/ai/status')
}

export function fetchAIConfig(): Promise<AIConfigSummary> {
  return fetchApi<AIConfigSummary>('/ai/config')
}

export function updateAIConfig(provider: string, model: string): Promise<AIConfigSummary> {
  return fetchApi<AIConfigSummary>('/ai/config', {
    method: 'POST',
    body: JSON.stringify({ provider, model }),
  })
}

// ── Session management ───────────────────────────────────────

export interface AISessionResponse {
  session_id: string
  project_root: string
  mode: string
}

export function createAISession(mode: SessionMode = 'agent'): Promise<AISessionResponse> {
  return fetchApi<AISessionResponse>('/ai/session', {
    method: 'POST',
    body: JSON.stringify({ mode }),
  })
}

export function listAISessions(): Promise<unknown[]> {
  return fetchApi<unknown[]>('/ai/sessions')
}

export function deleteAISession(sessionId: string): Promise<{ deleted: boolean }> {
  return fetchApi(`/ai/session/${sessionId}`, { method: 'DELETE' })
}

// ── Messages ─────────────────────────────────────────────────

export interface AIMessagePart {
  type: string
  text: string
}

export function sendAIMessage(
  sessionId: string,
  parts: AIMessagePart[],
): Promise<{ ok: boolean }> {
  return fetchApi<{ ok: boolean }>(`/ai/session/${sessionId}/message`, {
    method: 'POST',
    body: JSON.stringify({ parts }),
  })
}

export function getSessionMessages(sessionId: string): Promise<unknown[]> {
  return fetchApi<unknown[]>(`/ai/session/${sessionId}/messages`)
}

// ── Title generation ─────────────────────────────────────────

/** Fire-and-forget: ask the backend to generate an LLM title for the session. */
export function generateSessionTitle(
  sessionId: string,
  parts: AIMessagePart[],
): Promise<{ ok: boolean; title?: string }> {
  return fetchApi<{ ok: boolean; title?: string }>(
    `/ai/session/${sessionId}/generate-title`,
    {
      method: 'POST',
      body: JSON.stringify({ parts }),
    },
  )
}

// ── Cancel ───────────────────────────────────────────────────

export function cancelAIGeneration(sessionId: string): Promise<{ ok: boolean }> {
  return fetchApi<{ ok: boolean }>(`/ai/session/${sessionId}/cancel`, {
    method: 'POST',
  })
}

// ── Question tool response ──────────────────────────────────

/** Fetch pending question requests (for catching up after SSE reconnect). */
export function fetchPendingQuestions(): Promise<Array<{ id: string; sessionID: string }>> {
  return fetchApi<Array<{ id: string; sessionID: string }>>('/ai/questions')
}

/**
 * Reply to a question tool call.  OpenCode's question tool blocks
 * until it receives a structured answer via this endpoint.
 *
 * @param answers — parallel array matching `questions[]`.  Each
 *   element is a `string[]` of selected option labels (even for
 *   single-select, wrap in an array: `["Option A"]`).
 */
export function replyToQuestion(
  sessionId: string,
  requestId: string,
  answers: string[][],
): Promise<{ ok: boolean }> {
  return fetchApi<{ ok: boolean }>(
    `/ai/session/${sessionId}/question/${requestId}/reply`,
    { method: 'POST', body: JSON.stringify({ answers }) },
  )
}

/** Dismiss a question without answering. */
export function rejectQuestion(
  sessionId: string,
  requestId: string,
): Promise<{ ok: boolean }> {
  return fetchApi<{ ok: boolean }>(
    `/ai/session/${sessionId}/question/${requestId}/reject`,
    { method: 'POST' },
  )
}

// ── SSE event stream ─────────────────────────────────────────

/**
 * Connect to the SSE event stream for a session.
 *
 * Returns an EventSource that receives OpenCode events
 * proxied through the backend.  The caller is responsible
 * for closing it when done.
 */
export function connectAIEventStream(sessionId: string): EventSource {
  const baseUrl = import.meta.env.DEV ? '' : ''
  return new EventSource(`${baseUrl}/api/ai/session/${sessionId}/events`)
}

/**
 * Connect to the user-scoped SSE event stream (all sessions).
 *
 * Unlike connectAIEventStream, this receives events for ALL
 * sessions belonging to the user.  The frontend dispatches
 * events to the correct session tab using the sessionID field.
 */
export function connectAIUserEventStream(): EventSource {
  const baseUrl = import.meta.env.DEV ? '' : ''
  return new EventSource(`${baseUrl}/api/ai/events`)
}
