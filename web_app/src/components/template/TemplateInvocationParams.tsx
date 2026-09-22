import { useEffect, useId, useState } from 'react'
import type { TemplateAuthoringParameter } from '@/api/template-authoring'
import { initialParameterValue, formatTemplateValue } from '@/lib/template-document'
import { Button } from '@/components/ui/button'
import { ParameterValueInput } from './ParameterValueInput'

export function TemplateInvocationParams({ params, values, onSet, onUnset, onValidityChange, idPrefix, disabled = false }: {
  params: readonly TemplateAuthoringParameter[]
  values: Record<string, unknown>
  onSet: (name: string, value: unknown) => void
  onUnset: (name: string) => void
  onValidityChange?: (valid: boolean) => void
  idPrefix?: string
  disabled?: boolean
}) {
  const instanceId = useId()
  const [invalid, setInvalid] = useState<Set<string>>(new Set())
  useEffect(() => { onValidityChange?.(!params.some((param) => Object.hasOwn(values, param.name) && invalid.has(param.name))) }, [invalid, params, values, onValidityChange])
  if (!params.length) return <p className="text-xs text-muted-foreground">This template declares no parameters.</p>
  return <div className="space-y-3">{params.map((param, index) => {
    const supplied = Object.hasOwn(values, param.name)
    const id = `${idPrefix ?? instanceId}-input-${index}`
    return <section id={`${idPrefix ?? instanceId}-parameter-${index}`} tabIndex={-1} key={`${index}:${param.name}`} className="space-y-2 rounded-md border border-border p-3" aria-label={`Parameter ${param.name}`}>
      <div className="flex flex-wrap items-center justify-between gap-2">
        <span className="font-mono text-sm">{param.name} {param.required && <span className="font-sans text-xs text-muted-foreground">Required</span>}</span>
        <Button size="sm" variant="outline" disabled={disabled} onClick={() => supplied ? onUnset(param.name) : onSet(param.name, param.has_default ? structuredClone(param.default) : initialParameterValue(param.declared_type ?? undefined))}>
          {supplied ? param.required ? 'Omit value' : param.has_default ? 'Use default' : 'Omit value' : param.required ? param.has_default ? 'Supply declared default' : 'Supply value' : 'Override'}
        </Button>
      </div>
      {param.description && <p className="text-xs text-muted-foreground">{param.description}</p>}
      {!supplied && <p className={`text-xs ${param.required ? 'text-warning' : 'text-muted-foreground'}`}>
        {param.required ? 'Supply this key explicitly. A declared default does not satisfy Required.' : param.has_default ? 'Omitted: LHP will apply the declared default.' : 'Omitted: this parameter has no declared default.'}
      </p>}
      {param.has_default && <p className="break-all text-xs text-muted-foreground">Declared default: <code>{formatTemplateValue(param.default)}</code></p>}
      {supplied && <ParameterValueInput id={id} label={`Value for ${param.name}`} value={values[param.name]} onChange={(value) => onSet(param.name, value)} disabled={disabled}
        declaredType={param.declared_type ?? undefined}
        onValidityChange={(valid) => setInvalid((previous) => {
          if (previous.has(param.name) === !valid) return previous
          const next = new Set(previous); if (valid) next.delete(param.name); else next.add(param.name); return next
        })} />}
    </section>
  })}</div>
}
