import { fetchApi } from './client'
import type { DatasetLineageResponse } from '../types/api'

// `GET /api/lineage?env=&fqn=` — both are query params (a sink `fqn` contains
// `/`, which cannot be a path segment). 400 on missing/unknown env, 404 on an
// unknown fqn (incl. delta-sink lookups by their underlying table name, which
// are indexed under `sink:<type>/<id>` — see the backend module docstring).
export function fetchLineage(env: string, fqn: string): Promise<DatasetLineageResponse> {
  const params = new URLSearchParams({ env, fqn })
  return fetchApi(`/lineage?${params.toString()}`)
}
