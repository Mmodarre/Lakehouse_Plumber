import { fetchApi } from './client'
import type {
  EnvironmentListResponse,
  SubstitutionResolvedResponse,
} from '../types/api'

export function fetchEnvironments(): Promise<EnvironmentListResponse> {
  return fetchApi('/environments')
}

export function fetchEnvironmentResolved(
  env: string,
): Promise<SubstitutionResolvedResponse> {
  return fetchApi(`/environments/${encodeURIComponent(env)}/resolved`)
}
