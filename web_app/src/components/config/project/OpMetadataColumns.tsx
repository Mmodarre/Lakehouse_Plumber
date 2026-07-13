import { useState } from 'react'
import { Plus, X } from 'lucide-react'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { isPlainObject, parseLaxBool } from '../../../lib/config-model'
import { BoolSwitch } from '../fields/BoolSwitch'
import { OptionalTextField } from '../fields/OptionalTextField'
import { StringListEditor } from '../fields/StringListEditor'
import { SectionIssues } from './SectionIssues'
import type { ProjectFormApi } from './projectFormSupport'
import { issueText, issuesAtExactly } from './projectFormSupport'

// operational_metadata.columns — column definitions, including the parser's
// bare-string shorthand (`_col: F.expr(...)` where the string IS the
// expression, _operational_metadata_config_parser.py:24-26). Shorthand
// fidelity rule: editing the EXPRESSION keeps the shorthand (a surgical
// scalar patch); only editing a field beyond it converts the column to
// object form — an untouched shorthand column is never normalized.

const COLUMNS_PATH = ['operational_metadata', 'columns'] as const

function ColumnEditor({
  form,
  name,
  value,
  count,
}: {
  form: ProjectFormApi
  name: string
  value: unknown
  count: number
}) {
  const base = [...COLUMNS_PATH, name]
  const shorthand = typeof value === 'string'
  const spec = isPlainObject(value) ? value : {}
  const broken = !shorthand && !isPlainObject(value)

  const setField = (key: string, v: unknown) => {
    if (shorthand) {
      if (key === 'expression') form.set(base, v) // stays shorthand — surgical
      else form.set(base, { expression: value, [key]: v }) // convert to object form
    } else {
      form.setField(base, key, v)
    }
  }
  const delField = (key: string) => {
    if (shorthand) {
      if (key === 'expression') form.set(base, '')
    } else {
      form.del([...base, key])
    }
  }
  const remove = () => (count === 1 ? form.del([...COLUMNS_PATH]) : form.del(base))

  const appliesTo = Array.isArray(spec.applies_to) ? spec.applies_to : undefined
  const imports = Array.isArray(spec.additional_imports) ? spec.additional_imports : undefined
  const expression = shorthand ? value : spec.expression

  return (
    <div role="group" aria-label={name} className="space-y-2 rounded-md border border-border p-3">
      <div className="flex items-center justify-between gap-2">
        <p className="font-mono text-2xs font-medium">
          {name}
          {shorthand && (
            <Badge variant="outline" className="ml-2 rounded-sm px-1 text-2xs font-normal">
              shorthand
            </Badge>
          )}
        </p>
        <Button
          type="button"
          variant="ghost"
          size="icon-sm"
          onClick={remove}
          aria-label={`Remove column ${name}`}
        >
          <X aria-hidden="true" />
        </Button>
      </div>
      <SectionIssues issues={issuesAtExactly(form.issues, base)} />
      {!broken && (
        <>
          <OptionalTextField
            id={`om-column-${name}-expression`}
            label="Expression"
            value={expression}
            onSet={(v) => setField('expression', v)}
            onUnset={() => delField('expression')}
            monospace
            issue={issueText(form.issues, [...base, 'expression'])?.message}
          />
          <OptionalTextField
            id={`om-column-${name}-description`}
            label="Description"
            value={spec.description}
            onSet={(v) => setField('description', v)}
            onUnset={() => delField('description')}
          />
          <StringListEditor
            id={`om-column-${name}-applies-to`}
            label="Applies to"
            helpPath={[...base, 'applies_to']}
            value={appliesTo}
            monospace
            placeholder="e.g. streaming_table"
            onEditItem={(i, v) => form.set([...base, 'applies_to', i], v)}
            onAddItem={(v) =>
              appliesTo === undefined
                ? setField('applies_to', [v])
                : form.set([...base, 'applies_to', appliesTo.length], v)
            }
            onRemoveItem={(i) => form.del([...base, 'applies_to', i])}
            onDeleteKey={() => delField('applies_to')}
            issue={issueText(form.issues, [...base, 'applies_to'])?.message}
          />
          <StringListEditor
            id={`om-column-${name}-imports`}
            label="Additional imports"
            helpPath={[...base, 'additional_imports']}
            value={imports}
            monospace
            placeholder="e.g. from pyspark.sql import functions as F"
            onEditItem={(i, v) => form.set([...base, 'additional_imports', i], v)}
            onAddItem={(v) =>
              imports === undefined
                ? setField('additional_imports', [v])
                : form.set([...base, 'additional_imports', imports.length], v)
            }
            onRemoveItem={(i) => form.del([...base, 'additional_imports', i])}
            onDeleteKey={() => delField('additional_imports')}
            issue={issueText(form.issues, [...base, 'additional_imports'])?.message}
          />
          <BoolSwitch
            id={`om-column-${name}-enabled`}
            label="Enabled"
            value={'enabled' in spec ? parseLaxBool(spec.enabled) : undefined}
            defaultValue={true}
            onSet={(v) => setField('enabled', v)}
            onReset={() => delField('enabled')}
          />
        </>
      )}
    </div>
  )
}

export function OpMetadataColumns({
  form,
  columns,
}: {
  form: ProjectFormApi
  columns: Record<string, unknown> | undefined
}) {
  const [newName, setNewName] = useState('')
  const [nameError, setNameError] = useState<string | null>(null)
  const names = columns !== undefined ? Object.keys(columns) : []

  const addColumn = () => {
    const name = newName.trim()
    if (name === '') return
    if (columns !== undefined && name in columns) {
      setNameError(`Column '${name}' already exists`)
      return
    }
    setNameError(null)
    // New columns start as the bare-string shorthand (empty expression).
    if (columns === undefined) form.setField(['operational_metadata'], 'columns', { [name]: '' })
    else form.set([...COLUMNS_PATH, name], '')
    setNewName('')
  }

  return (
    <div className="space-y-2">
      <p className="text-xs font-medium text-foreground">Columns</p>
      <SectionIssues issues={issuesAtExactly(form.issues, [...COLUMNS_PATH])} />
      {names.map((name) => (
        <ColumnEditor
          key={name}
          form={form}
          name={name}
          value={columns?.[name]}
          count={names.length}
        />
      ))}
      <div className="flex items-center gap-1.5">
        <Input
          value={newName}
          onChange={(e) => {
            setNewName(e.target.value)
            setNameError(null)
          }}
          onKeyDown={(e) => {
            if (e.key === 'Enter') {
              e.preventDefault()
              addColumn()
            }
          }}
          placeholder="new column name"
          spellCheck={false}
          autoComplete="off"
          className="max-w-56 font-mono text-xs"
          aria-label="New column name"
        />
        <Button
          type="button"
          variant="outline"
          size="sm"
          onClick={addColumn}
          disabled={newName.trim() === ''}
        >
          <Plus aria-hidden="true" />
          Add column
        </Button>
      </div>
      {nameError && (
        <p role="alert" className="text-2xs text-destructive">
          {nameError}
        </p>
      )}
    </div>
  )
}
