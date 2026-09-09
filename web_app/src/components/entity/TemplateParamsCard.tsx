import { useMemo, useState } from 'react'
import { Plus, Trash2 } from 'lucide-react'
import { addTemplateParam, deleteTemplateParam, deleteTemplateParamField, readFlowgroupValue, setTemplateParamField, type TemplateParamRead } from '@/lib/flowgroup-doc'
import { hasTemplateDefault, initialParameterValue, referencesForParameter, templateParameterIssues, type TemplateReference } from '@/lib/template-document'
import { Button } from '@/components/ui/button'
import { BoolSwitch } from '@/components/config/fields/BoolSwitch'
import { DraftInput } from '@/components/config/fields/DraftInput'
import { EnumSelect } from '@/components/config/fields/EnumSelect'
import { descriptionIds } from '@/components/config/fields/fieldSupport'
import { FieldChrome } from '@/components/config/fields/FieldChrome'
import { ParameterValueInput } from '@/components/template/ParameterValueInput'
import { AlertDialog, AlertDialogAction, AlertDialogCancel, AlertDialogContent, AlertDialogDescription, AlertDialogFooter, AlertDialogHeader, AlertDialogTitle } from '@/components/ui/alert-dialog'
import type { FlowgroupMutator } from './useFlowgroupDoc'

const PARAM_TYPES = ['string', 'object', 'array', 'boolean', 'number'] as const
export interface TemplateParamsCardProps {
  params: readonly TemplateParamRead[]
  templateName: string
  commit: (mutator: FlowgroupMutator) => boolean
  readOnly: boolean
  references?: readonly TemplateReference[]
  onOpenCode?: () => void
}
const NO_REFERENCES: readonly TemplateReference[] = []
export function TemplateParamsCard({ params, templateName, commit, readOnly, references = NO_REFERENCES, onOpenCode }: TemplateParamsCardProps) {
  const [query, setQuery] = useState('')
  const [editNonce, setEditNonce] = useState(0)
  const [pending, setPending] = useState<{ param: TemplateParamRead; nextName?: string } | null>(null)
  const [nameError, setNameError] = useState<{ index: number; message: string }>()
  const issues = useMemo(() => templateParameterIssues(params), [params])
  const requestChange = (param: TemplateParamRead, nextName?: string) => {
    if (readOnly) return
    if (nextName !== undefined) {
      if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(nextName)) { setNameError({ index: param.index, message: 'Use letters, numbers and underscores, starting with a letter or underscore.' }); return }
      if (params.some((p) => p.index !== param.index && p.name === nextName)) { setNameError({ index: param.index, message: 'Another parameter already uses this name.' }); return }
      if (nextName === param.name) return
    }
    setNameError(undefined)
    setPending({ param, nextName })
  }
  const affected = pending ? referencesForParameter(references, pending.param.name) : []
  return <section aria-label="Template parameter declarations" className="space-y-4">
    <div className="flex flex-wrap items-start justify-between gap-3">
      <div><h2 className="text-sm font-semibold">Parameters ({params.length})</h2><p className="mt-1 max-w-2xl text-xs text-muted-foreground">Declare the inputs supplied by flowgroups using {templateName}. Action values reference them with <code>{'{{ name }}'}</code>.</p></div>
      <Button type="button" variant="outline" size="sm" disabled={readOnly} onClick={() => commit((body) => addTemplateParam(body, { name: nextParamName(params), type: 'string', required: true }))}><Plus aria-hidden="true" />Add parameter</Button>
    </div>
    {params.length > 3 && <input aria-label="Search parameters" placeholder="Find a parameter or description…" value={query} onChange={(e) => setQuery(e.target.value)} className="w-full rounded-md border border-input bg-background px-3 py-2 text-sm" />}
    {params.length === 0 && <p className="rounded-md border border-dashed border-border p-4 text-sm text-muted-foreground">Start with an input that changes between flowgroups, such as a table name or source path.</p>}
    <div className="space-y-3">{params.filter((p) => `${p.name} ${p.description ?? ''}`.toLowerCase().includes(query.toLowerCase())).map((param) => {
      const id = `tparam-${param.index}`
      const uses = referencesForParameter(references, param.name)
      const issue = nameError?.index === param.index ? nameError.message : issues.find((i) => i.index === param.index)?.message
      return <article key={`${param.index}-${editNonce}`} className="space-y-3 rounded-md border border-border bg-card p-4">
        <div className="flex items-start gap-2"><div className="min-w-0 flex-1" onKeyDownCapture={(event) => { if (event.key === 'Escape') setNameError(undefined) }}><FieldChrome id={`${id}-name`} label="Parameter name" helpPath={['parameters', param.index, 'name']} issue={issue}><DraftInput key={param.name} id={`${id}-name`} aria-describedby={descriptionIds(`${id}-name`)} aria-invalid={nameError?.index === param.index} initial={param.name} monospace disabled={readOnly} onCommit={(name) => requestChange(param, name.trim())} /></FieldChrome></div><Button variant="ghost" size="icon-sm" className="mt-5" disabled={readOnly} aria-label={`Delete parameter ${param.name}`} onClick={() => requestChange(param)}><Trash2 aria-hidden="true" /></Button></div>
        <FieldChrome id={`${id}-description`} label="Description" helpPath={['parameters', param.index, 'description']} description="Help the person using this template choose the right value."><DraftInput id={`${id}-description`} aria-describedby={descriptionIds(`${id}-description`)} initial={param.description ?? ''} disabled={readOnly} onCommit={(value) => commit((body) => value ? setTemplateParamField(body, param.index, ['description'], value) : deleteTemplateParamField(body, param.index, ['description']))} /></FieldChrome>
        <div className="grid gap-4 sm:grid-cols-2"><EnumSelect id={`${id}-type`} label="Declared type (advisory)" helpPath={['parameters', param.index, 'type']} value={param.type} options={param.type && !PARAM_TYPES.includes(param.type as typeof PARAM_TYPES[number]) ? [...PARAM_TYPES, param.type] : PARAM_TYPES} unsetLabel="Unspecified" disabled={readOnly} onSet={(value) => commit((body) => setTemplateParamField(body, param.index, ['type'], value))} onUnset={() => commit((body) => deleteTemplateParamField(body, param.index, ['type']))} />
          <BoolSwitch id={`${id}-required`} label="Required" helpPath={['parameters', param.index, 'required']} description="Every invoking flowgroup must supply this key, even when a default is declared." value={param.required} defaultValue={false} disabled={readOnly} onSet={(value) => commit((body) => setTemplateParamField(body, param.index, ['required'], value))} onReset={() => commit((body) => deleteTemplateParamField(body, param.index, ['required']))} />
        </div>
        <div className="rounded-md border border-border p-3"><label className="flex items-center gap-2 text-xs font-medium"><input type="checkbox" checked={hasTemplateDefault(param)} disabled={readOnly} onChange={(e) => commit((body) => e.target.checked ? setTemplateParamField(body, param.index, ['default'], initialParameterValue(param.type)) : deleteTemplateParamField(body, param.index, ['default']))} />Set a default value</label>
          <p className="my-2 text-xs text-muted-foreground">{hasTemplateDefault(param) ? 'Used when an optional parameter is omitted. Clearing text keeps an explicit empty string.' : 'No default is declared. This is different from a default of null, false or an empty value.'}</p>
          {hasTemplateDefault(param) && <ParameterValueInput id={`${id}-default`} label="Default" helpPath={['parameters', param.index, 'default']} value={param.default} declaredType={param.type} disabled={readOnly} onChange={(value) => commit((body) => setTemplateParamField(body, param.index, ['default'], value))} />}
        </div>
        <details className="text-xs"><summary className="cursor-pointer text-muted-foreground">Show uses ({uses.length}{uses.some((ref) => ref.kind === 'complex') ? ', including possible uses' : ''})</summary><ReferenceList references={uses} /><p className="mt-2 text-muted-foreground">Complex Jinja expressions require review. Flowgroups outside this draft are not rewritten.</p>{onOpenCode && <Button type="button" variant="link" size="xs" onClick={onOpenCode}>Review expressions in Code</Button>}</details>
      </article>
    })}</div>
    <AlertDialog open={!!pending} onOpenChange={(open) => { if (!open) { setPending(null); setEditNonce((n) => n + 1) } }}><AlertDialogContent><AlertDialogHeader><AlertDialogTitle>{pending?.nextName ? `Rename ${pending.param.name} to ${pending.nextName}?` : `Delete parameter ${pending?.param.name}?`}</AlertDialogTitle><AlertDialogDescription>The declaration changes in this draft. Existing action expressions and invoking flowgroups keep their current references; update them deliberately before using this template.</AlertDialogDescription></AlertDialogHeader><div className="max-h-56 overflow-auto"><ReferenceList references={affected} />{references.some((ref) => ref.kind !== 'direct') && <p className="mt-2 text-xs text-muted-foreground">This draft also contains expressions that static inspection cannot resolve completely.</p>}</div><AlertDialogFooter><AlertDialogCancel>Keep declaration</AlertDialogCancel><AlertDialogAction disabled={readOnly} onClick={() => {
      if (!pending) return
      commit((body) => { if (readFlowgroupValue(body, ['parameters', pending.param.index, 'name']) !== pending.param.raw.name) throw new Error('Parameter changed while reviewing.'); if (pending.nextName) setTemplateParamField(body, pending.param.index, ['name'], pending.nextName); else deleteTemplateParam(body, pending.param.index) })
      setPending(null); setEditNonce((n) => n + 1)
    }}>{pending?.nextName ? 'Rename declaration' : 'Delete declaration'}</AlertDialogAction></AlertDialogFooter></AlertDialogContent></AlertDialog>
  </section>
}
export function ReferenceList({ references }: { references: readonly TemplateReference[] }) {
  return references.length ? <ul className="mt-2 space-y-2">{references.map((ref, i) => <li key={i} className="rounded border border-border p-2"><code className="break-all">{ref.path.join('.')}</code><pre className="mt-1 whitespace-pre-wrap break-all text-muted-foreground">{ref.expression}</pre>{ref.kind !== 'direct' && <span className="text-muted-foreground">{ref.kind === 'complex' ? 'Possible use; review expression' : 'Not rendered by the current runtime'}</span>}</li>)}</ul> : <p className="mt-2 text-xs text-muted-foreground">No direct uses found in this template's action values.</p>
}
function nextParamName(params: readonly TemplateParamRead[]): string { const taken = new Set(params.map((p) => p.name)); for (let i = 1; ; i++) { const candidate = i === 1 ? 'parameter' : `parameter_${i}`; if (!taken.has(candidate)) return candidate } }
