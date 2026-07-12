import { fetchApi } from './client'
import type {
  GraphLevel,
  GraphResponse,
  ExecutionOrderResponse,
  CircularDependencyResponse,
  StalenessResponse,
  CrossPipelineSummary,
} from '../types/api'

export function fetchDependencyGraph(
  level: GraphLevel,
  pipeline?: string,
): Promise<GraphResponse> {
  const params = pipeline ? `?pipeline=${encodeURIComponent(pipeline)}` : ''
  return fetchApi(`/dependencies/graph/${level}${params}`)
}

/**
 * Compact cross-pipeline / external badge data for ONE pipeline. Replaces the
 * old whole-project graph fetch the drill view used only to derive badges —
 * the server returns just the per-flowgroup cross-pipeline connections.
 */
export function fetchCrossPipelineConnections(
  pipeline: string,
): Promise<CrossPipelineSummary> {
  return fetchApi(`/dependencies/cross-pipeline?pipeline=${encodeURIComponent(pipeline)}`)
}

export function fetchExecutionOrder(pipeline?: string): Promise<ExecutionOrderResponse> {
  const params = pipeline ? `?pipeline=${encodeURIComponent(pipeline)}` : ''
  return fetchApi(`/dependencies/execution-order${params}`)
}

export function fetchCircularDeps(): Promise<CircularDependencyResponse> {
  return fetchApi('/dependencies/circular')
}

/**
 * Report whether the served dependency graph is stale (O(1)) — reads the
 * server's per-app `graph_stale` flag set by the file watcher on a
 * graph-relevant edit. Used to seed the client stale flag on first load.
 */
export function fetchStaleness(pipeline?: string): Promise<StalenessResponse> {
  const params = pipeline ? `?pipeline=${encodeURIComponent(pipeline)}` : ''
  return fetchApi(`/dependencies/staleness${params}`)
}

/**
 * Force a fresh dependency-graph build (bypassing the serve-stale caches),
 * clear the server stale flag, and return the new build's metadata. This is
 * the write behind the SPA's "Refresh" affordance; the rebuild can take a
 * few seconds.
 */
export function refreshDependencies(pipeline?: string): Promise<StalenessResponse> {
  const params = pipeline ? `?pipeline=${encodeURIComponent(pipeline)}` : ''
  return fetchApi(`/dependencies/refresh${params}`, { method: 'POST' })
}
