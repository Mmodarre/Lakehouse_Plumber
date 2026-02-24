/**
 * OpenCode SDK client wrapper (singleton).
 *
 * Wraps all SDK interactions in a single module so that:
 * 1. SDK version changes only affect this file
 * 2. The client is initialized lazily and reused
 * 3. Response parsing is centralized
 *
 * OpenCode v1.2.10 uses synchronous JSON request/response
 * (not SSE streaming). The prompt endpoint returns immediately
 * once the LLM finishes generating.
 */

let _client: OpenCodeClient | null = null

export interface OpenCodeClient {
  baseUrl: string
  password?: string
}

/** A single part in an OpenCode response. */
export interface OpenCodePart {
  type: string // 'text', 'reasoning', 'tool-call', 'tool-result', 'step-start', 'step-finish'
  text?: string
  name?: string
  args?: Record<string, unknown>
  result?: string
  isError?: boolean
  [key: string]: unknown
}

/** Full response from POST /session/{id}/message. */
export interface OpenCodeResponse {
  info: Record<string, unknown>
  parts: OpenCodePart[]
}

/**
 * Initialize the OpenCode client.
 * Called once when AI status reports available=true.
 */
export function initOpenCodeClient(url: string, password?: string): OpenCodeClient {
  _client = { baseUrl: url.replace(/\/$/, ''), password }
  return _client
}

export function getOpenCodeClient(): OpenCodeClient | null {
  return _client
}

export function resetOpenCodeClient(): void {
  _client = null
}

/**
 * Build common request headers (Content-Type + optional auth).
 */
function buildHeaders(client: OpenCodeClient): Record<string, string> {
  const headers: Record<string, string> = {
    'Content-Type': 'application/json',
  }
  if (client.password) {
    headers['Authorization'] = `Bearer ${client.password}`
  }
  return headers
}

/**
 * Send a message to an OpenCode session and return the parsed response.
 *
 * OpenCode v1.2.10 endpoint: POST /session/{id}/message
 * Returns synchronous JSON with an `info` object and `parts` array.
 */
export async function sendMessage(
  client: OpenCodeClient,
  sessionId: string,
  text: string,
): Promise<OpenCodeResponse> {
  const resp = await fetch(`${client.baseUrl}/session/${sessionId}/message`, {
    method: 'POST',
    headers: buildHeaders(client),
    body: JSON.stringify({
      parts: [{ type: 'text', text }],
    }),
  })

  if (!resp.ok) {
    const detail = await resp.text().catch(() => '')
    throw new Error(
      `OpenCode message failed: ${resp.status} ${resp.statusText}${detail ? ` — ${detail}` : ''}`,
    )
  }
  return resp.json()
}

/**
 * Create a new session on the OpenCode server.
 */
export async function createOpenCodeSession(
  client: OpenCodeClient,
): Promise<{ id: string }> {
  const resp = await fetch(`${client.baseUrl}/session`, {
    method: 'POST',
    headers: buildHeaders(client),
    body: JSON.stringify({}),
  })

  if (!resp.ok) {
    throw new Error(`Failed to create OpenCode session: ${resp.status}`)
  }
  return resp.json()
}

/**
 * List existing sessions on the OpenCode server.
 */
export async function listSessions(
  client: OpenCodeClient,
): Promise<{ id: string }[]> {
  const resp = await fetch(`${client.baseUrl}/session`, {
    method: 'GET',
    headers: buildHeaders(client),
  })

  if (!resp.ok) {
    throw new Error(`Failed to list OpenCode sessions: ${resp.status}`)
  }
  return resp.json()
}

/**
 * Cancel an in-progress generation.
 */
export async function cancelGeneration(
  client: OpenCodeClient,
  sessionId: string,
): Promise<void> {
  await fetch(`${client.baseUrl}/session/${sessionId}/cancel`, {
    method: 'POST',
    headers: {
      ...(client.password ? { Authorization: `Bearer ${client.password}` } : {}),
    },
  })
}
