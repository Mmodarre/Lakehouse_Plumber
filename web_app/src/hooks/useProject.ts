import { useQuery } from '@tanstack/react-query'
import { fetchProject, fetchHealth, fetchStats } from '../api/project'

export function useProject() {
  return useQuery({
    queryKey: ['project'],
    queryFn: fetchProject,
  })
}

export function useHealth() {
  return useQuery({
    queryKey: ['health'],
    queryFn: fetchHealth,
    refetchInterval: 30000, // poll every 30s
    retry: 1,
  })
}

export function useStats() {
  return useQuery({
    queryKey: ['stats'],
    queryFn: fetchStats,
  })
}
