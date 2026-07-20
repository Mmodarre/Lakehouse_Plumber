import { fetchApi } from './client'

/**
 * Schema kinds served by the backend at `GET /api/schemas/{kind}`.
 *
 * Each kind maps to a packaged JSON schema in `src/lhp/schemas/{kind}.schema.json`
 * (the backend router resolves these via importlib.resources, so `kind` is the
 * filename stem before `.schema.json`).
 */
export type SchemaKind =
  | 'flowgroup'
  | 'preset'
  | 'template'
  | 'substitution'
  | 'project'
  | 'pipeline_config'
  | 'job_config'
  | 'schema'

/**
 * Fetch a canonical packaged JSON schema by kind.
 *
 * The schema is returned as an opaque JSON object suitable for handing
 * directly to monaco-yaml as an inline `schema`. The shape is not validated
 * here — it mirrors whatever the backend serves from the packaged
 * `*.schema.json` files.
 */
export function fetchSchema(kind: SchemaKind): Promise<Record<string, unknown>> {
  return fetchApi(`/schemas/${encodeURIComponent(kind)}`)
}

/**
 * Per-session promise cache over {@link fetchSchema}, keyed by kind.
 *
 * Guarantees a single network GET per `kind` for the lifetime of the module
 * so Monaco YAML validation and the field-help resolver share one document.
 * A rejected fetch is evicted so a transient error is not cached permanently.
 */
const schemaCache = new Map<SchemaKind, Promise<Record<string, unknown>>>()

export function loadSchemaCached(
  kind: SchemaKind,
): Promise<Record<string, unknown>> {
  const cached = schemaCache.get(kind)
  if (cached) return cached

  const p = fetchSchema(kind).catch((err) => {
    schemaCache.delete(kind)
    throw err
  })
  schemaCache.set(kind, p)
  return p
}
