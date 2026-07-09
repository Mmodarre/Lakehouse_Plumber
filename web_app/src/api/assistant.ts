import { ApiError, fetchApi } from './client'
import { getToken } from '../lib/session-token'
import type { ErrorDetail } from '../types/api'
import type {
  ApprovalRequestBody,
  AssistantStatus,
  AssistantSuccessResponse,
  ChatRequest,
  DaemonStartResponse,
  DatabricksProfilesResponse,
  ExecutorConfig,
  ExecutorConfigUpdate,
  SessionListResponse,
  SessionSnapshot,
  SkillInstallResponse,
} from '../types/assistant'

// ── Assistant panel API (/api/assistant/*) ───────────────
//
// Typed client for the P5 assistant endpoints. All JSON endpoints go
// through `fetchApi`; the one NDJSON endpoint (`POST /assistant/chat`)
// mirrors `startStream` in ./stream — POST with body + X-LHP-Token,
// returning the raw Response for the consuming hook to read.

const BASE_URL = import.meta.env.VITE_API_BASE_URL ?? '/api'

export function fetchAssistantStatus(): Promise<AssistantStatus> {
  return fetchApi('/assistant/status')
}

/**
 * Stored executor config, or `null` when none has been set yet (the
 * backend 404s in that case; absence is an expected state, not an error).
 */
export async function fetchExecutorConfig(): Promise<ExecutorConfig | null> {
  try {
    return await fetchApi<ExecutorConfig>('/assistant/config')
  } catch (err) {
    if (err instanceof ApiError && err.status === 404) return null
    throw err
  }
}

export function putExecutorConfig(
  body: ExecutorConfigUpdate,
): Promise<ExecutorConfig> {
  return fetchApi('/assistant/config', {
    method: 'PUT',
    body: JSON.stringify(body),
  })
}

export function fetchDatabricksProfiles(): Promise<DatabricksProfilesResponse> {
  return fetchApi('/assistant/databricks-profiles')
}

export function installAssistantSkill(): Promise<SkillInstallResponse> {
  return fetchApi('/assistant/skill', { method: 'POST' })
}

export function startAssistantDaemon(): Promise<DaemonStartResponse> {
  return fetchApi('/assistant/daemon/start', { method: 'POST' })
}

export function resolveAssistantApproval(
  body: ApprovalRequestBody,
): Promise<AssistantSuccessResponse> {
  return fetchApi('/assistant/approval', {
    method: 'POST',
    body: JSON.stringify(body),
  })
}

export function interruptAssistant(): Promise<AssistantSuccessResponse> {
  return fetchApi('/assistant/interrupt', { method: 'POST' })
}

export function fetchAssistantSession(): Promise<SessionSnapshot> {
  return fetchApi('/assistant/session')
}

export function fetchAssistantSessions(): Promise<SessionListResponse> {
  return fetchApi('/assistant/sessions')
}

export function newAssistantSession(): Promise<AssistantSuccessResponse> {
  return fetchApi('/assistant/session/new', { method: 'POST' })
}

/**
 * Open one assistant chat turn as an NDJSON stream over POST.
 *
 * Mirrors `startStream` in ./stream (POST-with-body + X-LHP-Token): returns
 * the raw {@link Response} so the consuming hook reads `response.body` as a
 * `ReadableStream` of newline-delimited `AssistantFrame`s. Non-2xx opens
 * throw {@link ApiError} — including the chat-gate 409s (`LHP-WEB-001`
 * executor unconfigured, `LHP-WEB-002` skill not installed, `LHP-WEB-003`
 * host offline), whose `ErrorDetail` envelope carries the gate code.
 */
export async function startAssistantChat(
  body: ChatRequest,
  signal?: AbortSignal,
): Promise<Response> {
  const url = `${BASE_URL}/assistant/chat`
  const token = getToken()
  const response = await fetch(url, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      Accept: 'application/x-ndjson',
      ...(token ? { 'X-LHP-Token': token } : {}),
    },
    body: JSON.stringify(body),
    signal,
  })

  if (!response.ok) {
    let detail: ErrorDetail | undefined
    try {
      const errorBody = await response.json()
      detail = errorBody.error ?? errorBody
    } catch {
      // non-JSON error response
    }

    if (detail?.code) {
      throw new ApiError(response.status, detail)
    }
    throw new ApiError(response.status, {
      code: 'UNKNOWN_ERROR',
      category: 'unknown',
      message: `Chat request failed: ${response.status} ${response.statusText}`,
      details: '',
      suggestions: [],
      context: {},
      http_status: response.status,
    })
  }

  return response
}
