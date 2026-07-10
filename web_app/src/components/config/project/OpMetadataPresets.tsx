import { useState } from 'react'
import { Plus, X } from 'lucide-react'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { isPlainObject } from '../../../lib/config-model'
import { OptionalTextField } from '../fields/OptionalTextField'
import { StringListEditor } from '../fields/StringListEditor'
import { SectionIssues } from './SectionIssues'
import type { ProjectFormApi } from './projectFormSupport'
import { issuesAtExactly } from './projectFormSupport'

// operational_metadata.presets — named column bundles, including the
// parser's bare-list shorthand (`preset: [col_a, col_b]` where the list IS
// the columns list, _operational_metadata_config_parser.py:62-64). Item
// edits patch the scalar in place, so an untouched shorthand preset is
// never normalized; adding a description converts to object form.

const PRESETS_PATH = ['operational_metadata', 'presets'] as const

function PresetEditor({
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
  const base = [...PRESETS_PATH, name]
  const shorthand = Array.isArray(value)
  const spec = isPlainObject(value) ? value : {}
  const broken = !shorthand && !isPlainObject(value)
  const columns = shorthand
    ? value
    : Array.isArray(spec.columns)
      ? spec.columns
      : undefined
  const listPath = shorthand ? base : [...base, 'columns']

  const remove = () => (count === 1 ? form.del([...PRESETS_PATH]) : form.del(base))

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
          aria-label={`Remove preset ${name}`}
        >
          <X aria-hidden="true" />
        </Button>
      </div>
      <SectionIssues issues={issuesAtExactly(form.issues, base)} />
      {!broken && (
        <>
          <StringListEditor
            id={`om-preset-${name}-columns`}
            label="Columns"
            description="Column names this preset selects — each must be defined above."
            value={columns}
            monospace
            allowEmpty
            placeholder="e.g. _ingested_at"
            onEditItem={(i, v) => form.set([...listPath, i], v)}
            onAddItem={(v) =>
              columns === undefined
                ? form.setField(base, 'columns', [v])
                : form.set([...listPath, columns.length], v)
            }
            onRemoveItem={(i) => form.del([...listPath, i])}
            onDeleteKey={() => form.del(listPath)}
          />
          <OptionalTextField
            id={`om-preset-${name}-description`}
            label="Description"
            value={spec.description}
            onSet={(v) =>
              shorthand
                ? form.set(base, { columns: value, description: v }) // convert to object form
                : form.setField(base, 'description', v)
            }
            onUnset={() => {
              if (!shorthand) form.del([...base, 'description'])
            }}
          />
        </>
      )}
    </div>
  )
}

export function OpMetadataPresets({
  form,
  presets,
}: {
  form: ProjectFormApi
  presets: Record<string, unknown> | undefined
}) {
  const [newName, setNewName] = useState('')
  const [nameError, setNameError] = useState<string | null>(null)
  const names = presets !== undefined ? Object.keys(presets) : []

  const addPreset = () => {
    const name = newName.trim()
    if (name === '') return
    if (presets !== undefined && name in presets) {
      setNameError(`Preset '${name}' already exists`)
      return
    }
    setNameError(null)
    // New presets start as the bare-list shorthand (empty column list).
    if (presets === undefined) form.setField(['operational_metadata'], 'presets', { [name]: [] })
    else form.set([...PRESETS_PATH, name], [])
    setNewName('')
  }

  return (
    <div className="space-y-2">
      <p className="text-xs font-medium text-foreground">Presets</p>
      <SectionIssues issues={issuesAtExactly(form.issues, [...PRESETS_PATH])} />
      {names.map((name) => (
        <PresetEditor
          key={name}
          form={form}
          name={name}
          value={presets?.[name]}
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
              addPreset()
            }
          }}
          placeholder="new preset name"
          spellCheck={false}
          autoComplete="off"
          className="max-w-56 font-mono text-xs"
          aria-label="New preset name"
        />
        <Button
          type="button"
          variant="outline"
          size="sm"
          onClick={addPreset}
          disabled={newName.trim() === ''}
        >
          <Plus aria-hidden="true" />
          Add preset
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
