import { fetchApi } from './client'
import type {
  GraphLevel,
  GraphResponse,
  ExecutionOrderResponse,
  CircularDependencyResponse,
} from '../types/api'

export function fetchDependencyGraph(
  level: GraphLevel,
  pipeline?: string,
): Promise<GraphResponse> {
  const params = pipeline ? `?pipeline=${encodeURIComponent(pipeline)}` : ''
  return fetchApi(`/dependencies/graph/${level}${params}`)
}

export function fetchExecutionOrder(pipeline?: string): Promise<ExecutionOrderResponse> {
  const params = pipeline ? `?pipeline=${encodeURIComponent(pipeline)}` : ''
  return fetchApi(`/dependencies/execution-order${params}`)
}

export function fetchCircularDeps(): Promise<CircularDependencyResponse> {
  return fetchApi('/dependencies/circular')
}
