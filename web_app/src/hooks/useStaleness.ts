import { useQuery } from '@tanstack/react-query'
import { fetchStaleness } from '../api/state'

export function useStaleness(env: string) {
  return useQuery({
    queryKey: ['staleness', env],
    queryFn: () => fetchStaleness(env),
  })
}
