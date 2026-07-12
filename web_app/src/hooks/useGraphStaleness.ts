import { useEffect } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { toast } from 'sonner'
import { fetchStaleness, refreshDependencies } from '../api/dependencies'
import { errorMessage } from '../lib/errors'
import { useGraphStalenessStore } from '../store/graphStalenessStore'

// ── useGraphStaleness — read the stale flag + drive a manual refresh ──
//
// The client stale flag lives in graphStalenessStore and is set by the
// push channel on a `graph-stale` event. This hook exposes it to the graph
// toolbars and owns the "Refresh" action: POST /api/dependencies/refresh
// (a whole-project rebuild that clears the server flag), then invalidate
// the graph-derived query keys so every open graph view refetches, and
// clear the client flag.
//
// It also seeds the flag once from GET /api/dependencies/staleness so a
// freshly loaded SPA reflects staleness that accrued before this session
// (edits made while the page was closed). The seed is one-way: it only
// ever SETS the flag, never clears it — a live `graph-stale` mark must
// never be clobbered by a cached seed, and only a refresh clears staleness.

export interface UseGraphStalenessResult {
  /** True while the served graph is known-stale. Drives the Refresh badge. */
  isStale: boolean
  /** Force a rebuild + refetch and clear the flag. */
  refresh: () => void
  /** True while the refresh POST is in flight. */
  isRefreshing: boolean
}

export function useGraphStaleness(): UseGraphStalenessResult {
  const queryClient = useQueryClient()
  const isStale = useGraphStalenessStore((s) => s.isStale)
  const markStale = useGraphStalenessStore((s) => s.markStale)
  const clearStale = useGraphStalenessStore((s) => s.clearStale)

  // One-way seed: reflect server staleness on first load / after reconnect.
  const { data: seed } = useQuery({
    queryKey: ['graph-staleness'],
    queryFn: () => fetchStaleness(),
  })

  useEffect(() => {
    if (seed?.stale) markStale()
  }, [seed, markStale])

  const mutation = useMutation({
    mutationFn: () => refreshDependencies(),
    onSuccess: (data) => {
      clearStale()
      // Keep the seed cache consistent so a later remount does not re-mark
      // stale from a pre-refresh cached value.
      queryClient.setQueryData(['graph-staleness'], data)
      void queryClient.invalidateQueries({ queryKey: ['dep-graph'] })
      void queryClient.invalidateQueries({ queryKey: ['execution-order'] })
      void queryClient.invalidateQueries({ queryKey: ['circular-deps'] })
    },
    onError: (err) => {
      // The rebuild can fail (e.g. an LHP-VAL-* error). Surface it and leave
      // the stale flag SET — the graph is still stale, so the badge must stay.
      // isPending flips back to false automatically, re-enabling the button.
      toast.error(errorMessage(err, 'Failed to refresh the dependency graph'))
    },
  })

  return {
    isStale,
    refresh: () => mutation.mutate(),
    isRefreshing: mutation.isPending,
  }
}
