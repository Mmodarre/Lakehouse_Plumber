import { useQuery } from '@tanstack/react-query'
import { fetchPresets, fetchPresetDetail } from '../api/presets'

export function usePresets() {
  return useQuery({
    queryKey: ['presets'],
    queryFn: fetchPresets,
  })
}

export function usePresetDetail(name: string | null) {
  return useQuery({
    queryKey: ['preset', name],
    queryFn: () => fetchPresetDetail(name!),
    enabled: !!name,
  })
}
