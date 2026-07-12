import { BoolSwitch } from '../config/fields/BoolSwitch'
import { KeyValueMapEditor } from '../config/fields/KeyValueMapEditor'
import { OptionalNumberField } from '../config/fields/OptionalNumberField'
import { OptionalTextField } from '../config/fields/OptionalTextField'
import { StringListEditor } from '../config/fields/StringListEditor'

// ── ParamsForm — declared template / blueprint parameters ────
//
// Renders a template's or blueprint's declared parameters as type-appropriate
// controls, reusing the Config-UI field widgets. Values live in the parent
// dialog: each control commits through `onSet(name, value)` / `onUnset(name)`.
// Soft hint only — a missing required value shows a note but the write side
// (`lhp validate`) stays the authority.

export interface ParamDecl {
  name: string
  /** 'string' | 'number' | 'boolean' | 'object' | 'array'; absent → string. */
  type?: string
  required?: boolean
  default?: unknown
  description?: string
}

export interface ParamsFormProps {
  params: ParamDecl[]
  values: Record<string, unknown>
  onSet: (name: string, value: unknown) => void
  onUnset: (name: string) => void
  disabled?: boolean
}

function placeholderFor(param: ParamDecl): string | undefined {
  if (param.default === undefined || param.default === null) return undefined
  if (typeof param.default === 'object') return undefined
  return String(param.default)
}

export function ParamsForm({ params, values, onSet, onUnset, disabled }: ParamsFormProps) {
  if (params.length === 0) {
    return (
      <p className="text-2xs text-muted-foreground">
        This selection declares no parameters — nothing to fill in.
      </p>
    )
  }
  return (
    <div className="flex flex-col gap-3">
      {params.map((param) => (
        <ParamField
          key={param.name}
          param={param}
          value={values[param.name]}
          onSet={(v) => onSet(param.name, v)}
          onUnset={() => onUnset(param.name)}
          disabled={disabled}
        />
      ))}
    </div>
  )
}

interface ParamFieldProps {
  param: ParamDecl
  value: unknown
  onSet: (value: unknown) => void
  onUnset: () => void
  disabled?: boolean
}

function ParamField({ param, value, onSet, onUnset, disabled }: ParamFieldProps) {
  const id = `param-${param.name}`
  const missing =
    param.required && (value === undefined || value === null || value === '')
  const issue = missing ? `${param.name} is required.` : undefined
  const label = param.required ? `${param.name} *` : param.name

  switch (param.type) {
    case 'boolean':
      return (
        <BoolSwitch
          id={id}
          label={label}
          value={typeof value === 'boolean' ? value : undefined}
          defaultValue={param.default === true}
          onSet={onSet}
          onReset={onUnset}
          description={param.description}
          issue={issue}
          disabled={disabled}
        />
      )
    case 'number':
      return (
        <OptionalNumberField
          id={id}
          label={label}
          value={value}
          onSet={onSet}
          onUnset={onUnset}
          description={param.description}
          placeholder={placeholderFor(param)}
          issue={issue}
          disabled={disabled}
        />
      )
    case 'object':
      return (
        <KeyValueMapEditor
          id={id}
          label={label}
          value={isRecord(value) ? value : undefined}
          onSetEntry={(key, val) => onSet({ ...(isRecord(value) ? value : {}), [key]: val })}
          onRenameEntry={(oldKey, newKey) => {
            const base = isRecord(value) ? { ...value } : {}
            const held = base[oldKey]
            delete base[oldKey]
            base[newKey] = held
            onSet(base)
          }}
          onRemoveEntry={(key) => {
            const base = isRecord(value) ? { ...value } : {}
            delete base[key]
            onSet(base)
          }}
          onDeleteKey={onUnset}
          allowEmpty
          description={param.description}
          disabled={disabled}
        />
      )
    case 'array':
      return (
        <StringListEditor
          id={id}
          label={label}
          value={Array.isArray(value) ? value : undefined}
          onEditItem={(index, val) => {
            const base = Array.isArray(value) ? [...value] : []
            base[index] = val
            onSet(base)
          }}
          onAddItem={(val) => onSet([...(Array.isArray(value) ? value : []), val])}
          onRemoveItem={(index) => {
            const base = Array.isArray(value) ? [...value] : []
            base.splice(index, 1)
            onSet(base)
          }}
          onDeleteKey={onUnset}
          allowEmpty
          description={param.description}
          monospace
          disabled={disabled}
        />
      )
    default:
      return (
        <OptionalTextField
          id={id}
          label={label}
          value={value}
          onSet={onSet}
          onUnset={onUnset}
          description={param.description}
          placeholder={placeholderFor(param)}
          monospace
          issue={issue}
          issueSeverity="warning"
          disabled={disabled}
        />
      )
  }
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
}
