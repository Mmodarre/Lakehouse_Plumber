import { create } from 'zustand'

// ── graphStalenessStore — the dependency-graph freshness flag ──
//
// The server serves the last-good dependency graph even while stale and
// publishes a `graph-stale` push event on any graph-relevant edit (see
// usePushChannel). Rather than refetch the whole graph on every edit, the
// SPA flips this single app-wide flag and surfaces a "Refresh" affordance;
// the flag is cleared only by a successful POST /api/dependencies/refresh.
//
// Module-level (not per-view) on purpose: the push channel is mounted once
// at Layout, so the flag is set even when no graph view is open — a graph
// opened later already reflects the pending staleness.

interface GraphStalenessState {
  isStale: boolean
  /** Mark the graph stale (idempotent). Called on the `graph-stale` event
   *  and when the server-seed reports staleness on first load. */
  markStale: () => void
  /** Clear the flag after a successful manual refresh. */
  clearStale: () => void
}

export const useGraphStalenessStore = create<GraphStalenessState>((set) => ({
  isStale: false,
  markStale: () => set({ isStale: true }),
  clearStale: () => set({ isStale: false }),
}))
