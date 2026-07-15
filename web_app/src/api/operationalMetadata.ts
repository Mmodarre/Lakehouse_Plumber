import { fetchApi } from './client'
import type { OperationalMetadataResponse } from '../types/api'

// `GET /api/operational-metadata` — the operational-metadata columns and
// presets available to the current project (REPLACE semantics; see the backend
// router in src/lhp/webapp/routers/metadata.py). Read-only.
export function fetchOperationalMetadata(): Promise<OperationalMetadataResponse> {
  return fetchApi('/operational-metadata')
}
