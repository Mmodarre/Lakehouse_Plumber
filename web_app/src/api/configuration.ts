import type { components } from '../types/api.generated'
import { fetchApi } from './client'

type Schemas = components['schemas']
export type ConfigurationPreview = Schemas['ConfigurationPreviewResponse']

export function fetchConfigurationPreview(
  path: string,
  kind: 'pipeline' | 'job',
  env: string,
  target: string,
): Promise<ConfigurationPreview> {
  const params = new URLSearchParams({ path, kind, env, target })
  return fetchApi(`/configuration/preview?${params}`)
}
