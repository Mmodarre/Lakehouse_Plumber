import { fetchApi } from './client'
import type {
  FlowgroupListResponse,
  FlowgroupDetailResponse,
  FlowgroupRelatedFilesResponse,
  ResolvedFlowgroupResponse,
} from '../types/api'

export function fetchFlowgroups(pipeline?: string): Promise<FlowgroupListResponse> {
  const params = pipeline ? `?pipeline=${encodeURIComponent(pipeline)}` : ''
  return fetchApi(`/flowgroups${params}`)
}

export function fetchFlowgroupDetail(name: string): Promise<FlowgroupDetailResponse> {
  return fetchApi(`/flowgroups/${encodeURIComponent(name)}`)
}

export function fetchFlowgroupRelatedFiles(
  name: string,
  env: string,
): Promise<FlowgroupRelatedFilesResponse> {
  return fetchApi(
    `/flowgroups/${encodeURIComponent(name)}/related-files?env=${encodeURIComponent(env)}`,
  )
}

export function fetchFlowgroupResolved(
  name: string,
  env: string,
): Promise<ResolvedFlowgroupResponse> {
  return fetchApi(
    `/flowgroups/${encodeURIComponent(name)}/resolved?environment=${encodeURIComponent(env)}`,
  )
}
