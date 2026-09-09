import { useState, type ReactNode } from 'react'
import { descriptionIds } from '@/components/config/fields/fieldSupport'
import { DraftInput } from '@/components/config/fields/DraftInput'
import { FieldChrome } from '@/components/config/fields/FieldChrome'
import { Button } from '@/components/ui/button'
import type { SchemaPath } from '@/lib/schema-help'
import { useTemplateParameters } from './TemplateParameterContext'

export function TemplateFieldBinding({ id, label, value, disabled, onSet, onUnset, helpPath, children }: {
  id: string; label: string; value: unknown; disabled: boolean; onSet: (value: unknown) => void; onUnset: () => void; helpPath: SchemaPath; children: ReactNode
}) {
  const params = useTemplateParameters()
  const bound = typeof value === 'string' && (value.includes('{{') || value.includes('{%'))
  const [requestedExpression, setRequestedExpression] = useState(false)
  if (params === null) return children
  const expression = bound || requestedExpression
  return <div className="space-y-1.5 rounded-md border border-transparent">
    <div className="flex flex-wrap items-center justify-end gap-2 text-xs">
      <span className="text-muted-foreground">Value mode</span>
      <Button type="button" variant={expression ? 'ghost' : 'secondary'} size="xs" disabled={disabled || bound} onClick={() => setRequestedExpression(false)}>Literal</Button>
      <Button type="button" variant={expression ? 'secondary' : 'ghost'} size="xs" disabled={disabled} onClick={() => setRequestedExpression(true)}>Expression</Button>
    </div>
    {expression ? <FieldChrome id={`${id}-expression`} label={label} helpPath={helpPath} description="Template inputs use {{ name }}. Environment substitutions use ${name}. Sample values are applied in Preview.">
      <DraftInput id={`${id}-expression`} aria-describedby={descriptionIds(`${id}-expression`)} initial={typeof value === 'string' ? value : ''} multiline monospace disabled={disabled} onCommit={(text) => text === '' ? onUnset() : onSet(text)} />
      <div className="mt-2 flex flex-wrap items-center gap-2">
        <select aria-label={`Insert parameter into ${label}`} value="" disabled={disabled || params.length === 0} className="max-w-full rounded-md border border-input bg-background px-2 py-1 text-xs" onChange={(event) => {
          const name = event.target.value
          if (!name) return
          const input = document.getElementById(`${id}-expression`)
          const text = input instanceof HTMLTextAreaElement || input instanceof HTMLInputElement ? input.value : typeof value === 'string' ? value : ''
          const start = input instanceof HTMLTextAreaElement || input instanceof HTMLInputElement ? input.selectionStart ?? text.length : text.length
          const end = input instanceof HTMLTextAreaElement || input instanceof HTMLInputElement ? input.selectionEnd ?? start : start
          const insertion = `{{ ${name} }}`
          onSet(text.slice(0, start) + insertion + text.slice(end))
        }}><option value="">Insert parameter…</option>{params.filter((p, index) => /^[A-Za-z_][A-Za-z0-9_]*$/.test(p.name) && params.findIndex((q) => q.name === p.name) === index).map((p) => <option key={p.name} value={p.name}>{p.name}{p.type ? ` · ${p.type}` : ''}{p.description ? ` — ${p.description}` : ''}</option>)}</select>
        <Button type="button" variant="ghost" size="xs" disabled={disabled} onClick={() => { onUnset(); setRequestedExpression(false) }}>Clear expression and use literal</Button>
        {params.length === 0 && <span className="text-xs text-muted-foreground">Declare an input in Parameters to insert it here.</span>}
      </div>
    </FieldChrome> : children}
  </div>
}
