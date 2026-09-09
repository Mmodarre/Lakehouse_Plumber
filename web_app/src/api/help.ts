import { fetchApi } from './client'
import type { SchemaKind } from './schemas'
import type { HelpCatalog } from '../lib/field-help'

const cache = new Map<string, Promise<HelpCatalog>>()
const kinds = new Set<SchemaKind>(['project', 'pipeline_config', 'job_config', 'flowgroup', 'template'])

/** Shared by forms and Monaco; failures are evicted so a later view can retry. */
export function loadHelpCached(kind: SchemaKind): Promise<HelpCatalog> {
  if (!kinds.has(kind)) return Promise.resolve({ version: 1, entries: [] })
  const existing = cache.get(kind)
  if (existing) return existing
  const request = fetchApi<HelpCatalog>(`/help/${encodeURIComponent(kind)}`).then((data) => {
    if (data.version !== 1 || !Array.isArray(data.entries)) throw new Error('Unsupported help catalog')
    return data
  }).catch((error: unknown) => {
    cache.delete(kind)
    throw error
  })
  cache.set(kind, request)
  return request
}
