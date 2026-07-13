/* eslint-disable react-refresh/only-export-components -- context module: the
   Provider and its consumer hook (useFieldHelp) are intentionally colocated. */
import { createContext, useContext, type ReactNode } from 'react'
import { useSchemaHelp, type FieldHelpResolver } from '../../hooks/useSchemaHelp'
import { type SchemaKind } from '../../api/schemas'
import { type SchemaPath } from '../../lib/schema-help'

const NOOP: FieldHelpResolver = () => undefined
const SchemaKindContext = createContext<FieldHelpResolver>(NOOP)

export function SchemaKindProvider({ kind, children }: { kind: SchemaKind; children: ReactNode }) {
  const resolver = useSchemaHelp(kind)
  return <SchemaKindContext.Provider value={resolver}>{children}</SchemaKindContext.Provider>
}

/** override wins (UI-only fields); else resolve from schema by path; else undefined. */
export function useFieldHelp(path?: SchemaPath, override?: string): string | undefined {
  const resolver = useContext(SchemaKindContext)
  if (override !== undefined) return override
  return path ? resolver(path) : undefined
}
