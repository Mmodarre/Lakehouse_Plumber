import { useMemo } from 'react'
import { useQuery } from '@tanstack/react-query'
import { loadSchemaCached, type SchemaKind } from '../api/schemas'
import { loadHelpCached } from '../api/help'
import { buildSchemaHelpResolver, type SchemaPath } from '../lib/schema-help'
import { resolveFieldHelp, type ResolvedHelp } from '../lib/field-help'

export type FieldHelpResolver = (path: SchemaPath) => ResolvedHelp | undefined
const HELP_ROOT: Partial<Record<SchemaKind, string>> = {
  project: '#', pipeline_config: '#/definitions/PipelineSettings',
  job_config: '#/definitions/JobSettings', flowgroup: '#/definitions/Action',
}

export function useSchemaHelp(kind: SchemaKind, subtype?: string): FieldHelpResolver {
  const { data: schema } = useQuery({
    queryKey: ['schema', kind], queryFn: () => loadSchemaCached(kind), staleTime: Infinity,
  })
  const { data: catalog } = useQuery({
    queryKey: ['field-help', kind], queryFn: () => loadHelpCached(kind), staleTime: Infinity, retry: false,
  })
  const root = HELP_ROOT[kind] ?? '#'
  return useMemo(() => {
    const fallback = schema ? buildSchemaHelpResolver(schema, root) : () => undefined
    return (path: SchemaPath) => {
      const rich = catalog ? resolveFieldHelp(catalog, path, subtype) : undefined
      if (rich) return rich
      const summary = fallback(path)
      return summary ? { summary } : undefined
    }
  }, [catalog, schema, root, subtype])
}
