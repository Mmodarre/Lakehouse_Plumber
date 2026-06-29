import { useQuery } from '@tanstack/react-query'
import { fetchEnvironments } from '../api/environments'

export function useEnvironments() {
  return useQuery({
    queryKey: ['environments'],
    queryFn: fetchEnvironments,
  })
}
