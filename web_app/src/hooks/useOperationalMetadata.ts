import { useQuery } from '@tanstack/react-query'
import { fetchOperationalMetadata } from '../api/operationalMetadata'

// Operational-metadata columns/presets available to the project, keyed
// `['operational-metadata']` per the house queryKey convention. Invalidated by
// persistBufferToDisk on a write to the project-root lhp.yaml (where these are
// declared). Consumed by the MetadataMultiSelect widget.
export function useOperationalMetadata() {
  return useQuery({
    queryKey: ['operational-metadata'],
    queryFn: fetchOperationalMetadata,
  })
}
