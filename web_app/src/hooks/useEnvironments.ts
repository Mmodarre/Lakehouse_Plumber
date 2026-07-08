import { useQuery } from '@tanstack/react-query'
import { fetchEnvironments, fetchEnvironmentResolved } from '../api/environments'

export function useEnvironments() {
  return useQuery({
    queryKey: ['environments'],
    queryFn: fetchEnvironments,
  })
}

export function useEnvironmentResolved(env: string | null) {
  return useQuery({
    queryKey: ['environment-resolved', env],
    queryFn: () => fetchEnvironmentResolved(env!),
    enabled: !!env,
  })
}
