import { fetchApi } from './client'
import type { TemplateListResponse, TemplateDetailResponse } from '../types/api'
import type { TemplateListDetailResponse } from '../components/builder/types/builder'

export function fetchTemplates(): Promise<TemplateListResponse> {
  return fetchApi('/templates')
}

export function fetchTemplatesDetail(): Promise<TemplateListDetailResponse> {
  return fetchApi('/templates?detail=true')
}

export function fetchTemplateDetail(name: string): Promise<TemplateDetailResponse> {
  return fetchApi(`/templates/${encodeURIComponent(name)}`)
}
