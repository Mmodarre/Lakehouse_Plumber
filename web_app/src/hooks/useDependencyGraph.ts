import { useQuery } from '@tanstack/react-query'
import { fetchDependencyGraph, fetchExecutionOrder, fetchCircularDeps } from '../api/dependencies'
import type { GraphLevel } from '../types/api'

export function useDependencyGraph(level: GraphLevel, pipeline?: string) {
  return useQuery({
    queryKey: ['dep-graph', level, pipeline],
    queryFn: () => fetchDependencyGraph(level, pipeline),
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
