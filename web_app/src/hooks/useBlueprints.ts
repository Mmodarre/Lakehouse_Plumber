import { useQuery } from '@tanstack/react-query'
import { fetchBlueprints } from '../api/blueprints'

export function useBlueprints(includeInstances = false) {
  return useQuery({
    queryKey: ['blueprints', includeInstances],
    queryFn: () => fetchBlueprints(includeInstances),
  })
}
