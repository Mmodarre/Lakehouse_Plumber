import { fetchApi } from './client'
import type { PresetListResponse, PresetDetailResponse } from '../types/api'

export function fetchPresets(): Promise<PresetListResponse> {
  return fetchApi('/presets')
}

export function fetchPresetDetail(name: string): Promise<PresetDetailResponse> {
  return fetchApi(`/presets/${encodeURIComponent(name)}`)
}
