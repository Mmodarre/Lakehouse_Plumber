/** API client for AI assistant endpoints. */

import { fetchApi } from './client'
import type { AIStatus } from '../types/chat'

export function fetchAIStatus(): Promise<AIStatus> {
  return fetchApi<AIStatus>('/ai/status')
}

export function createAISession(): Promise<{ session_id: string; project_root: string }> {
  return fetchApi('/ai/session', { method: 'POST' })
}

export function deleteAISession(sessionId: string): Promise<{ deleted: boolean }> {
  return fetchApi(`/ai/session/${sessionId}`, { method: 'DELETE' })
}
