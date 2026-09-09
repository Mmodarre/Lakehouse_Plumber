import { fetchApi } from './client'

import type { components } from '@/types/api.generated'
type Schemas = components['schemas']
export type TemplateDiagnostic = Schemas['TemplateDiagnostic']
export type TemplateAuthoringParameter = Schemas['TemplateAuthoringParameter']
export type TemplateCatalogEntry = Schemas['TemplateCatalogEntry']
export type TemplatePreviewContext = Required<Pick<Schemas['TemplatePreviewContext'], 'pipeline' | 'flowgroup' | 'environment'>> & Pick<Schemas['TemplatePreviewContext'], 'presets' | 'variables'>
export type TemplatePreviewRequest = Omit<Schemas['TemplatePreviewRequest'], 'sample_parameters' | 'context'> & {
  sample_parameters: Record<string, unknown>
  context?: TemplatePreviewContext
}
export type TemplatePreviewResponse = Schemas['TemplatePreviewResponse']
export function fetchTemplateCatalog() {
  return fetchApi<{ templates: TemplateCatalogEntry[]; total: number }>('/templates/catalog')
}
export function fetchTemplateSource(path: string) {
  return fetchApi<{ template: TemplateCatalogEntry }>(`/templates/source?path=${encodeURIComponent(path)}`)
}
export function previewTemplate(body: TemplatePreviewRequest, signal?: AbortSignal) {
  return fetchApi<TemplatePreviewResponse>('/templates/preview', {
    method: 'POST', body: JSON.stringify(body), signal,
  })
}
