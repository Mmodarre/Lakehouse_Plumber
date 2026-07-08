import { fetchApi } from './client'
import type {
  ProjectInfoResponse,
  HealthResponse,
  ProjectStatsResponse,
  InitProjectRequest,
  InitProjectResponse,
} from '../types/api'

export function fetchProject(): Promise<ProjectInfoResponse> {
  return fetchApi('/project')
}

export function fetchHealth(): Promise<HealthResponse> {
  return fetchApi('/health')
}

export function fetchStats(): Promise<ProjectStatsResponse> {
  return fetchApi('/project/stats')
}

// Valid only while health reports `project_state === 'no_project'`; the
// backend answers 409 once a project is already initialized.
export function initProject(
  body: InitProjectRequest,
): Promise<InitProjectResponse> {
  return fetchApi('/project/init', {
    method: 'POST',
    body: JSON.stringify(body),
  })
}
