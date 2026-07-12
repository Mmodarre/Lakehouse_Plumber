import { useQuery } from '@tanstack/react-query'
import { fetchSandboxScope } from '../api/sandbox'

// The sandbox scope view. Env-independent (no `env` param), so a single
// cache entry serves the header pill, the picker, and every scope filter.
// The picker invalidates ['sandbox'] after writing .lhp/profile.yaml.
export function useSandbox() {
  return useQuery({
    queryKey: ['sandbox'],
    queryFn: () => fetchSandboxScope(),
  })
}
