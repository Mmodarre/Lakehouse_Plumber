import { stringify } from 'yaml'
import { FormSection } from './FormSection'

interface ExtrasDisplayProps {
  extras?: Record<string, unknown>
}

/**
 * Read-only display of unknown/extra YAML fields that were found
 * during deserialization but don't map to any form field.
 * These are preserved on save via the _extras merge in useYAMLGenerator.
 */
export function ExtrasDisplay({ extras }: ExtrasDisplayProps) {
  if (!extras || Object.keys(extras).length === 0) return null

  return (
    <FormSection title="Additional Fields (read-only)">
      <pre className="whitespace-pre-wrap rounded-md border border-slate-200 bg-slate-50 p-3 font-mono text-xs text-slate-600">
        {stringify(extras, { indent: 2 }).trim()}
      </pre>
      <p className="text-[10px] text-slate-400">
        These fields are not editable in visual mode but will be preserved on save.
      </p>
    </FormSection>
  )
}
