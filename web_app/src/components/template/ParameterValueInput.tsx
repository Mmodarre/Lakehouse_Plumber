import { useEffect, useState } from 'react'
import { descriptionIds } from '@/components/config/fields/fieldSupport'
import type { SchemaPath } from '@/lib/schema-help'
import { DraftInput } from '@/components/config/fields/DraftInput'
import { FieldChrome } from '@/components/config/fields/FieldChrome'
import { Button } from '@/components/ui/button'
import { formatTemplateValue, initialParameterValue, parseTemplateValue } from '@/lib/template-document'

export interface ParameterValueInputProps {
  id: string
  label: string
  /** An explicit value. The parent owns whether the key is supplied at all. */
  value: unknown
  onChange: (value: unknown) => void
  onValidityChange?: (valid: boolean) => void
  disabled?: boolean
  declaredType?: string
  description?: string
  helpPath?: SchemaPath
  /** Bound nested structured forms; deeper values retain the YAML editor. */
  depth?: number
}
type ValueKind = 'string' | 'number' | 'boolean' | 'null' | 'array' | 'object'
function valueKind(value: unknown): ValueKind {
  return value === null ? 'null' : Array.isArray(value) ? 'array' : typeof value === 'object' ? 'object' : typeof value === 'number' ? 'number' : typeof value === 'boolean' ? 'boolean' : 'string'
}
export function ParameterValueInput({ id, label, value, onChange, onValidityChange, disabled, declaredType, description, helpPath, depth = 0 }: ParameterValueInputProps) {
  const kind = valueKind(value)
  const collection = kind === 'array' || kind === 'object'
  const [yamlMode, setYamlMode] = useState(false)
  const [newKey, setNewKey] = useState('')
  const [error, setError] = useState<string>()
  const [propertyError, setPropertyError] = useState<string>()
  const [invalidChildren, setInvalidChildren] = useState<ReadonlySet<string>>(new Set())
  const childKeys = collection && value ? Object.keys(value) : []
  const childrenValid = !childKeys.some((key) => invalidChildren.has(key))
  useEffect(() => { onValidityChange?.(!error && childrenValid) }, [error, childrenValid, onValidityChange])
  const advisory = declaredType && declaredType !== kind ? `Declared type is ${declaredType}; this value is ${kind}. Types are advisory metadata in LHP.` : undefined
  const parse = (text: string) => {
    const result = parseTemplateValue(text, kind === 'string' ? 'string' : 'yaml')
    if (!result.ok) return result
    if (valueKind(result.value) !== kind) return { ok: false as const, error: `Enter a ${kind} value, or change Value format first.` }
    return result
  }
  const commit = (text: string) => {
    const result = parse(text)
    if (!result.ok) { setError(result.error); onValidityChange?.(false); return }
    setError(undefined)
    onChange(result.value)
  }
  const childValidity = (key: string, valid: boolean) => setInvalidChildren((previous) => {
    if (previous.has(key) === !valid) return previous
    const next = new Set(previous)
    if (valid) next.delete(key); else next.add(key)
    return next
  })
  const structured = collection && !yamlMode && depth < 4
  const object = kind === 'object' ? value as Record<string, unknown> : {}
  const list = Array.isArray(value) ? value : []
  return <FieldChrome id={id} label={label} helpPath={helpPath} description={description} issue={error ?? advisory} issueSeverity={error ? 'error' : 'warning'}>
    <div className="space-y-2">
      <div className="flex flex-wrap items-center gap-2"><label className="flex flex-wrap items-center gap-2 text-xs text-muted-foreground">
        Value format
        <select aria-label={`${label} value format`} aria-describedby={`${descriptionIds(id)} ${id}-format-hint`} value={kind} disabled={disabled} className="rounded-md border border-input bg-background px-2 py-1 text-foreground" onChange={(event) => {
          const next = event.target.value as ValueKind
          setError(undefined); setPropertyError(undefined); setInvalidChildren(new Set()); setYamlMode(false)
          onChange(next === 'null' ? null : initialParameterValue(next))
        }}>
          <option value="string">Text</option><option value="number">Number</option><option value="boolean">Boolean</option><option value="null">Null</option><option value="array">List</option><option value="object">Object</option>
        </select>
      </label>{collection && depth < 4 && <Button type="button" size="xs" variant="ghost" disabled={!!error || !childrenValid} onClick={() => setYamlMode(!yamlMode)}>{yamlMode ? `Edit ${label} as fields` : `Edit ${label} as YAML`}</Button>}</div>
      <p id={`${id}-format-hint`} className="text-xs text-muted-foreground">Changing the value format starts a new empty value of that kind.</p>
      {structured ? <div id={id} role="group" aria-label={label} aria-describedby={descriptionIds(id)} className="space-y-3 rounded-md border border-border p-3">
        {kind === 'array' ? <>
          {list.length === 0 && <p className="text-xs text-muted-foreground">Empty list. This value is explicitly supplied as <code>[]</code>.</p>}
          {list.map((item, index) => <div key={index} className="space-y-1 border-b border-border pb-2 last:border-b-0"><div className="flex justify-end"><Button type="button" variant="ghost" size="xs" disabled={disabled} aria-label={`Remove ${label} item ${index + 1}`} onClick={() => { setInvalidChildren(new Set()); onChange(list.filter((_, i) => i !== index)) }}>Remove item</Button></div><ParameterValueInput id={`${id}-item-${index}`} label={`${label} item ${index + 1}`} value={item} onChange={(next) => onChange(list.map((previous, i) => i === index ? next : previous))} onValidityChange={(valid) => childValidity(String(index), valid)} disabled={disabled} depth={depth + 1} /></div>)}
          <Button type="button" size="xs" variant="outline" disabled={disabled} onClick={() => onChange([...list, ''])}>Add {label} item</Button>
        </> : <>
          {Object.keys(object).length === 0 && <p className="text-xs text-muted-foreground">Empty object. This value is explicitly supplied as <code>{'{}'}</code>.</p>}
          {Object.entries(object).map(([key, item], index) => <div key={key} className="space-y-2 border-b border-border pb-3 last:border-b-0"><div className="flex items-center gap-2"><label className="min-w-0 flex-1 text-xs" onKeyDownCapture={(event) => { if (event.key === 'Escape' && propertyError === key) { setError(undefined); setPropertyError(undefined) } }} onChangeCapture={(event) => {
            if (!(event.target instanceof HTMLInputElement)) return
            const next = event.target.value
            const invalid = !next.trim() || next !== key && Object.hasOwn(object, next)
            setPropertyError(invalid ? key : undefined)
            setError(invalid ? 'Property names must be nonempty and unique. Restore the name with Escape or enter another name.' : undefined)
          }}>Property name<DraftInput initial={key} aria-invalid={propertyError === key} aria-describedby={descriptionIds(id)} aria-label={`${label} property ${index + 1} name`} disabled={disabled} onCommit={(next) => {
            if (!next.trim() || next !== key && Object.hasOwn(object, next)) { setError('Property names must be nonempty and unique. Restore the name with Escape or enter another name.'); return }
            setError(undefined); setPropertyError(undefined); onChange(Object.fromEntries(Object.entries(object).map(([name, previous]) => [name === key ? next : name, previous])))
          }} /></label><Button type="button" size="xs" variant="ghost" disabled={disabled} aria-label={`Remove ${label} property ${key}`} onClick={() => { const next = { ...object }; delete next[key]; onChange(next) }}>Remove</Button></div><ParameterValueInput id={`${id}-property-${index}`} label={`${label}.${key}`} value={item} onChange={(next) => onChange({ ...object, [key]: next })} onValidityChange={(valid) => childValidity(key, valid)} disabled={disabled} depth={depth + 1} /></div>)}
          <div className="flex flex-wrap items-end gap-2"><label className="min-w-0 flex-1 text-xs">New property<input aria-label={`New ${label} property`} value={newKey} disabled={disabled} onChange={(event) => setNewKey(event.target.value)} onKeyDown={(event) => { if (event.key === 'Enter' && !event.nativeEvent.isComposing) { event.preventDefault(); if (newKey.trim() && !Object.hasOwn(object, newKey.trim())) { onChange({ ...object, [newKey.trim()]: '' }); setNewKey('') } } }} className="mt-1 w-full rounded border border-input bg-background px-2 py-1.5 font-mono text-xs" /></label><Button type="button" size="xs" variant="outline" disabled={disabled || !newKey.trim() || Object.hasOwn(object, newKey.trim())} onClick={() => { onChange({ ...object, [newKey.trim()]: '' }); setNewKey('') }}>Add property</Button></div>
          {!!newKey.trim() && Object.hasOwn(object, newKey.trim()) && <p className="text-xs text-warning">That property already exists.</p>}
        </>}
      </div> : kind === 'null' ? <p id={id} className="text-xs text-muted-foreground">Explicit <code>null</code>. The key is present.</p>
        : kind === 'boolean' ? <select id={id} value={String(value)} disabled={disabled} aria-label={label} aria-describedby={descriptionIds(id)} className="w-full rounded-md border border-input bg-background px-3 py-2 text-sm" onChange={(event) => onChange(event.target.value === 'true')}><option value="false">false</option><option value="true">true</option></select>
          : <div onKeyDownCapture={(event) => { if (event.key === 'Escape') { setError(undefined); onValidityChange?.(true) } }} onChangeCapture={(event) => { const target = event.target; if (target instanceof HTMLInputElement || target instanceof HTMLTextAreaElement) { const result = parse(target.value); setError(result.ok ? undefined : result.error); onValidityChange?.(result.ok) } }}><DraftInput key={kind} id={id} initial={kind === 'string' ? String(value ?? '') : formatTemplateValue(value)} onCommit={commit} disabled={disabled} monospace multiline={collection} aria-invalid={!!error} aria-describedby={descriptionIds(id)} /></div>}
      {collection && !structured && <p className="text-xs text-muted-foreground">Nested values are supported. Use {kind === 'array' ? '[]' : '{}'} for an explicit empty {kind === 'array' ? 'list' : 'object'}.</p>}
    </div>
  </FieldChrome>
}
