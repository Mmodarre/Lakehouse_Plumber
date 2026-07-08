import { fetchApi } from './client'
import type { BlueprintListResponse } from '../types/api'

export function fetchBlueprints(
  includeInstances = false,
): Promise<BlueprintListResponse> {
  return fetchApi(`/blueprints?include_instances=${includeInstances}`)
}
