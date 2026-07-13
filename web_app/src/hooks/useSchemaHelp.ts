import { useMemo } from 'react'
import { useQuery } from '@tanstack/react-query'
import { loadSchemaCached, type SchemaKind } from '../api/schemas'
import { buildSchemaHelpResolver, type SchemaPath } from '../lib/schema-help'

export type FieldHelpResolver = (path: SchemaPath) => string | undefined

// Per-kind JSON pointer to the node that form field paths are relative to.
const HELP_ROOT: Partial<Record<SchemaKind, string>> = {
  project: '#',
  pipeline_config: '#/definitions/PipelineSettings',
  job_config: '#/definitions/JobSettings',
  flowgroup: '#/definitions/Action',
}

const NOOP: FieldHelpResolver = () => undefined

export function useSchemaHelp(kind: SchemaKind): FieldHelpResolver {
  const { data } = useQuery({
    queryKey: ['schema', kind],
    queryFn: () => loadSchemaCached(kind),
    staleTime: Infinity,
  })
  const root = HELP_ROOT[kind] ?? '#'
  return useMemo(() => (data ? buildSchemaHelpResolver(data, root) : NOOP), [data, root])
}
