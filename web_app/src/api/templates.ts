import { fetchApi } from './client'
import type { TemplateListResponse, TemplateDetailResponse } from '../types/api'

export function fetchTemplates(): Promise<TemplateListResponse> {
  return fetchApi('/templates')
}

export function fetchTemplateDetail(name: string): Promise<TemplateDetailResponse> {
  return fetchApi(`/templates/${encodeURIComponent(name)}`)
}
