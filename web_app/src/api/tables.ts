import { fetchApi } from './client'
import type { TableListResponse } from '../types/api'

export function fetchTables(env: string, pipeline?: string): Promise<TableListResponse> {
  const params = new URLSearchParams({ env })
  if (pipeline) params.set('pipeline', pipeline)
  return fetchApi(`/tables?${params.toString()}`)
}
