import { fetchApi } from './client'
import type { RunListResponse, RunDetail } from '../types/api'

export function fetchRuns(limit = 50): Promise<RunListResponse> {
  return fetchApi(`/runs?limit=${limit}`)
}

export function fetchRun(
  runId: string,
  includeEvents = false,
): Promise<RunDetail> {
  return fetchApi(
    `/runs/${encodeURIComponent(runId)}?include_events=${includeEvents}`,
  )
}
