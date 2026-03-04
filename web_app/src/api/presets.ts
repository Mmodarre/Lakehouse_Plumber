import { fetchApi } from './client'
import type { PresetListResponse, PresetDetailResponse } from '../types/api'
import type { PresetListDetailResponse } from '../components/builder/types/builder'

export function fetchPresets(): Promise<PresetListResponse> {
  return fetchApi('/presets')
}

export function fetchPresetsDetail(): Promise<PresetListDetailResponse> {
  return fetchApi('/presets?detail=true')
}

export function fetchPresetDetail(name: string): Promise<PresetDetailResponse> {
  return fetchApi(`/presets/${encodeURIComponent(name)}`)
}
