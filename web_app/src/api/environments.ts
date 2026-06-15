import { fetchApi } from './client'
import type { EnvironmentListResponse } from '../types/api'

export function fetchEnvironments(): Promise<EnvironmentListResponse> {
  return fetchApi('/environments')
}
