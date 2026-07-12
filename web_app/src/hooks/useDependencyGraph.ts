import { useQuery } from '@tanstack/react-query'
import {
  fetchDependencyGraph,
  fetchExecutionOrder,
  fetchCircularDeps,
  fetchCrossPipelineConnections,
} from '../api/dependencies'
import type { GraphLevel } from '../types/api'
import { crossPipelineSummaryToMap } from '../utils/externalConnections'

export function useDependencyGraph(level: GraphLevel, pipeline?: string) {
  return useQuery({
    queryKey: ['dep-graph', level, pipeline],
    queryFn: () => fetchDependencyGraph(level, pipeline),
  })
}

/**
 * Cross-pipeline / external badge connections for a single pipeline, mapped to
 * the `nodeId → ExternalConnection[]` shape the badge layer consumes. Replaces
 * the drill view's old unscoped whole-project graph fetch: the badge data is
 * derived server-side and only the compact summary crosses the wire.
 */
export function useCrossPipelineConnections(pipeline: string) {
  return useQuery({
    queryKey: ['cross-pipeline', pipeline],
    queryFn: () => fetchCrossPipelineConnections(pipeline),
    select: crossPipelineSummaryToMap,
  })
}

export function useExecutionOrder(pipeline?: string) {
  return useQuery({
    queryKey: ['execution-order', pipeline],
    queryFn: () => fetchExecutionOrder(pipeline),
  })
}

export function useCircularDeps() {
  return useQuery({
    queryKey: ['circular-deps'],
    queryFn: fetchCircularDeps,
  })
}
