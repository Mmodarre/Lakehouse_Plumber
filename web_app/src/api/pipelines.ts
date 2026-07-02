import { fetchApi } from './client'
import type {
  PipelineListResponse,
  PipelineDetailResponse,
  PipelineFlowgroupsResponse,
} from '../types/api'

export function fetchPipelines(): Promise<PipelineListResponse> {
  return fetchApi('/pipelines')
}

export function fetchPipelineDetail(name: string): Promise<PipelineDetailResponse> {
  return fetchApi(`/pipelines/${encodeURIComponent(name)}`)
}

export function fetchPipelineFlowgroups(name: string): Promise<PipelineFlowgroupsResponse> {
  return fetchApi(`/pipelines/${encodeURIComponent(name)}/flowgroups`)
}
