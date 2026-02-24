import { useQuery } from '@tanstack/react-query'
import {
  fetchFlowgroups,
  fetchFlowgroupDetail,
  fetchFlowgroupRelatedFiles,
  fetchFlowgroupResolved,
} from '../api/flowgroups'

export function useFlowgroups(pipeline?: string) {
  return useQuery({
    queryKey: ['flowgroups', pipeline],
    queryFn: () => fetchFlowgroups(pipeline),
  })
}

export function useFlowgroupDetail(name: string | null) {
  return useQuery({
    queryKey: ['flowgroup', name],
    queryFn: () => fetchFlowgroupDetail(name!),
    enabled: !!name,
  })
}

export function useFlowgroupRelatedFiles(name: string | null, env: string) {
  return useQuery({
    queryKey: ['flowgroup-related-files', name, env],
    queryFn: () => fetchFlowgroupRelatedFiles(name!, env),
    enabled: !!name,
  })
}

export function useFlowgroupResolved(name: string | null, env: string) {
  return useQuery({
    queryKey: ['flowgroup-resolved', name, env],
    queryFn: () => fetchFlowgroupResolved(name!, env),
    enabled: !!name,
  })
}
