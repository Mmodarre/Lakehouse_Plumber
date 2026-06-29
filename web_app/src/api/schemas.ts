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
