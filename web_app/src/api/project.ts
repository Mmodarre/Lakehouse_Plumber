import { fetchApi } from './client'
import type { ProjectInfoResponse, HealthResponse, ProjectStatsResponse } from '../types/api'

export function fetchProject(): Promise<ProjectInfoResponse> {
  return fetchApi('/project')
}

export function fetchHealth(): Promise<HealthResponse> {
  return fetchApi('/health')
}

export function fetchStats(): Promise<ProjectStatsResponse> {
  return fetchApi('/project/stats')
}
