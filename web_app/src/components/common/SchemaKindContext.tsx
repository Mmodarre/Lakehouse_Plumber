/* eslint-disable react-refresh/only-export-components -- provider and consumer hooks */
import { createContext, useContext, type ReactNode } from 'react'
import { useSchemaHelp, type FieldHelpResolver } from '../../hooks/useSchemaHelp'
import type { SchemaKind } from '../../api/schemas'
import type { SchemaPath } from '../../lib/schema-help'
import type { ResolvedHelp } from '../../lib/field-help'

const SchemaKindContext = createContext<FieldHelpResolver>(() => undefined)
export function SchemaKindProvider({ kind, subtype, children }: { kind: SchemaKind; subtype?: string; children: ReactNode }) {
  const resolver = useSchemaHelp(kind, subtype)
  return <SchemaKindContext.Provider value={resolver}>{children}</SchemaKindContext.Provider>
}

export function useResolvedFieldHelp(path?: SchemaPath, override?: string): ResolvedHelp | undefined {
  const resolver = useContext(SchemaKindContext)
  const result = path ? resolver(path) : undefined
  if (override !== undefined) return override ? { ...result, summary: override } : undefined
  return result
}

/** Compatibility consumer for UI-only fields that only need the summary. */
export function useFieldHelp(path?: SchemaPath, override?: string): string | undefined {
  return useResolvedFieldHelp(path, override)?.summary
}
