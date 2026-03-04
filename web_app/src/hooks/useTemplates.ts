import { useQuery } from '@tanstack/react-query'
import { fetchTemplates, fetchTemplatesDetail, fetchTemplateDetail } from '../api/templates'

export function useTemplates() {
  return useQuery({
    queryKey: ['templates'],
    queryFn: fetchTemplates,
  })
}

export function useTemplatesDetail() {
  return useQuery({
    queryKey: ['templates-detail'],
    queryFn: fetchTemplatesDetail,
  })
}

export function useTemplateDetail(name: string | null) {
  return useQuery({
    queryKey: ['template', name],
    queryFn: () => fetchTemplateDetail(name!),
    enabled: !!name,
  })
}
