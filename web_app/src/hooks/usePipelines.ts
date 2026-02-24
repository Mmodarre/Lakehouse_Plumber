import { useQuery } from '@tanstack/react-query'
import { fetchPipelines, fetchPipelineDetail, fetchPipelineFlowgroups } from '../api/pipelines'

export function usePipelines() {
  return useQuery({
    queryKey: ['pipelines'],
    queryFn: fetchPipelines,
  })
}

export function usePipelineDetail(name: string | null) {
  return useQuery({
    queryKey: ['pipeline', name],
    queryFn: () => fetchPipelineDetail(name!),
    enabled: !!name,
  })
}

export function usePipelineFlowgroups(name: string | null) {
  return useQuery({
    queryKey: ['pipeline-flowgroups', name],
    queryFn: () => fetchPipelineFlowgroups(name!),
    enabled: !!name,
  })
}
