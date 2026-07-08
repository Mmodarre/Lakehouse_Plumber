import { useQuery } from '@tanstack/react-query'
import { fetchRuns, fetchRun } from '../api/runs'

// List key is ['run-history', limit]: usePushChannel invalidates the
// ['run-history'] prefix on run-updated push events, which matches every
// limit-parameterized instance of this query.
export function useRuns(limit = 50) {
  return useQuery({
    queryKey: ['run-history', limit],
    queryFn: () => fetchRuns(limit),
  })
}

export function useRun(runId: string | null, includeEvents = false) {
  return useQuery({
    queryKey: ['run', runId, includeEvents],
    queryFn: () => fetchRun(runId!, includeEvents),
    enabled: !!runId,
  })
}
