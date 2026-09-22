import { useQuery } from '@tanstack/react-query'
import { fetchTemplateCatalog, fetchTemplateSource } from '@/api/template-authoring'

export function useTemplateCatalog() {
  return useQuery({ queryKey: ['templates', 'catalog'], queryFn: fetchTemplateCatalog })
}
export function useTemplateSource(path: string | null) {
  return useQuery({ queryKey: ['template', path], queryFn: () => fetchTemplateSource(path!), enabled: !!path })
}
