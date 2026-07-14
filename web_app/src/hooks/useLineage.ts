import { useQuery } from '@tanstack/react-query'
import { fetchLineage } from '../api/lineage'

// Lineage for one produced dataset under an env. Keyed `['lineage', env, fqn]`
// per the house queryKey convention; disabled until both are present. The
// error path surfaces the backend 400 (unknown env) / 404 (unknown fqn) as an
// ApiError the view renders as a friendly empty/error state.
export function useLineage(env: string, fqn: string) {
  return useQuery({
    queryKey: ['lineage', env, fqn],
    queryFn: () => fetchLineage(env, fqn),
    enabled: !!env && !!fqn,
  })
}
