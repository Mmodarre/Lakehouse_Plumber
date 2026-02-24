import { fetchApi } from './client'
import type { StalenessResponse } from '../types/api'

export function fetchStaleness(env: string): Promise<StalenessResponse> {
  return fetchApi(`/state/staleness?environment=${encodeURIComponent(env)}`)
}
