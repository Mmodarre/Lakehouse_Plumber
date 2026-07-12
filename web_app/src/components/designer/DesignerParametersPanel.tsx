import { useState } from 'react'
import { parse as parseYaml } from 'yaml'
import { Braces, PanelLeftClose, PanelLeftOpen, Plus, Trash2 } from 'lucide-react'
import {
  addTemplateParam,
  deleteTemplateParam,
  deleteTemplateParamField,
  setTemplateParamField,
} from '@/lib/flowgroup-doc'
import type { TemplateParamRead } from '@/lib/flowgroup-doc'
import { Button } from '@/components/ui/button'
import { BoolSwitch } from '@/components/config/fields/BoolSwitch'
import { DraftInput } from '@/components/config/fields/DraftInput'
import { EnumSelect } from '@/components/config/fields/EnumSelect'
import { FieldChrome } from '@/components/config/fields/FieldChrome'
import type { DesignerMutator } from './useDesignerWrite'

// ── DesignerParametersPanel — declare a template's parameters ─
//
// The left rail of the designer in template-authoring mode. Each row declares
// one parameter (name / type / required / default / description) and writes
// straight through `commit` (immediate write-through, comment-preserving, and
// hard-blocked by the dirty guard like every other designer edit). A
// parameter name here is what `{{ name }}` in an action field binds to — the
// primary accent on each row echoes the "param" chip those fields show.

/** Declarable parameter types (models/_template.py). Order = display order. */
const PARAM_TYPES: readonly string[] = ['string', 'object', 'array', 'boolean', 'number']

export interface DesignerParametersPanelProps {
  params: TemplateParamRead[]
  templateName: string
  /** Apply a mutator to the template body + write through. */
  commit: (mutator: DesignerMutator) => void
  /** Disable every control (dirty text buffer or unresolved conflict). */
  readOnly: boolean
}

export function DesignerParametersPanel({
  params,
  templateName,
  commit,
  readOnly,
}: DesignerParametersPanelProps) {
  const [collapsed, setCollapsed] = useState(false)

  if (collapsed) {
    return (
      <aside className="flex w-10 shrink-0 flex-col items-center border-r border-border bg-card py-2">
        <Button
          variant="ghost"
          size="icon-sm"
          className="text-muted-foreground"
          onClick={() => setCollapsed(false)}
          aria-label="Show parameters"
          title="Show parameters"
        >
          <PanelLeftOpen aria-hidden="true" />
        </Button>
        <div className="mt-2 flex items-center gap-1 [writing-mode:vertical-rl]">
          <Braces className="size-3 text-primary" aria-hidden="true" />
          <span className="text-2xs font-semibold uppercase tracking-[0.05em] text-muted-foreground">
            Parameters ({params.length})
          </span>
        </div>
      </aside>
    )
  }

  const addParam = () =>
    commit((body) => addTemplateParam(body, { name: nextParamName(params) }))

  return (
    <aside
      aria-label="Template parameters"
      className="flex w-72 shrink-0 flex-col overflow-y-auto border-r border-border bg-card"
    >
      <div className="flex items-start justify-between gap-2 border-b border-border px-4 pb-3 pt-4">
        <div className="min-w-0">
          <div className="flex items-center gap-1.5 text-2xs font-semibold uppercase tracking-[0.05em] text-muted-foreground">
            <Braces className="size-3 text-primary" aria-hidden="true" />
            Parameters ({params.length})
          </div>
          <p className="mt-1 text-2xs text-muted-foreground">
            Inputs for <span className="font-mono text-foreground/80">{templateName}</span>. A
            field can reference one with <code className="font-mono text-primary">{'{{ name }}'}</code>.
          </p>
        </div>
        <Button
          variant="ghost"
          size="icon-sm"
          className="shrink-0 text-muted-foreground"
          onClick={() => setCollapsed(true)}
          aria-label="Hide parameters"
          title="Hide parameters"
        >
          <PanelLeftClose aria-hidden="true" />
        </Button>
      </div>

      <div className="flex flex-col gap-3 px-4 py-4">
        {params.length === 0 ? (
          <p className="text-2xs text-muted-foreground">No parameters declared yet.</p>
        ) : (
          <ul className="flex flex-col gap-3">
            {params.map((param) => (
              // Index keys: rows are positional slots in the YAML sequence.
              <ParamRow
                key={param.index}
                param={param}
                commit={commit}
                readOnly={readOnly}
              />
            ))}
          </ul>
        )}
        <Button
          type="button"
          variant="outline"
          size="sm"
          className="gap-1"
          onClick={addParam}
          disabled={readOnly}
        >
          <Plus aria-hidden="true" /> Add parameter
        </Button>
      </div>
    </aside>
  )
}

function ParamRow({
  param,
  commit,
  readOnly,
}: {
  param: TemplateParamRead
  commit: (mutator: DesignerMutator) => void
  readOnly: boolean
}) {
  const { index } = param
  const rowId = `tparam-${index}`
  const missingName = param.name === ''

  return (
    <li className="relative rounded-sm border border-border bg-background/40 p-2.5">
      {/* Primary rail ties the row to the {{ param }} chip hue. */}
      <span
        aria-hidden="true"
        className="absolute inset-y-1.5 left-0 w-0.5 rounded-full bg-primary/60"
      />
      <div className="flex items-center gap-1.5">
        <DraftInput
          id={`${rowId}-name`}
          initial={param.name}
          monospace
          disabled={readOnly}
          aria-label={`Parameter ${index + 1} name`}
          className="!text-sm font-medium"
          onCommit={(next) => {
            const name = next.trim()
            if (name === '' || name === param.name) return
            commit((body) => setTemplateParamField(body, index, ['name'], name))
          }}
        />
        <Button
          type="button"
          variant="ghost"
          size="icon-sm"
          className="shrink-0 text-muted-foreground hover:text-destructive"
          onClick={() => commit((body) => deleteTemplateParam(body, index))}
          disabled={readOnly}
          aria-label={`Delete parameter ${param.name || index + 1}`}
        >
          <Trash2 aria-hidden="true" />
        </Button>
      </div>
      {missingName && (
        <p className="mt-1 text-2xs text-warning">A parameter needs a name to be referenced.</p>
      )}

      <div className="mt-2 flex flex-col gap-2.5">
        <EnumSelect
          id={`${rowId}-type`}
          label="Type"
          value={param.type}
          options={PARAM_TYPES}
          unsetLabel="Unspecified"
          onSet={(next) => commit((body) => setTemplateParamField(body, index, ['type'], next))}
          onUnset={() => commit((body) => deleteTemplateParamField(body, index, ['type']))}
          disabled={readOnly}
        />

        <BoolSwitch
          id={`${rowId}-required`}
          label="Required"
          value={param.required}
          defaultValue={false}
          onSet={(checked) =>
            commit((body) =>
              checked
                ? setTemplateParamField(body, index, ['required'], true)
                : deleteTemplateParamField(body, index, ['required']),
            )
          }
          onReset={() => commit((body) => deleteTemplateParamField(body, index, ['required']))}
          disabled={readOnly}
        />

        <FieldChrome
          id={`${rowId}-default`}
          label="Default"
          description="Used when an instance omits this parameter. Empty clears it."
        >
          <DraftInput
            id={`${rowId}-default`}
            initial={formatDefault(param.default)}
            monospace
            placeholder={param.required ? 'none (required)' : 'none'}
            disabled={readOnly}
            onCommit={(next) => {
              if (next.trim() === '') {
                commit((body) => deleteTemplateParamField(body, index, ['default']))
                return
              }
              const value = parseDefault(next, param.type)
              commit((body) => setTemplateParamField(body, index, ['default'], value))
            }}
          />
        </FieldChrome>

        <FieldChrome id={`${rowId}-desc`} label="Description">
          <DraftInput
            id={`${rowId}-desc`}
            initial={param.description ?? ''}
            disabled={readOnly}
            placeholder="What this parameter controls"
            onCommit={(next) =>
              commit((body) =>
                next.trim() === ''
                  ? deleteTemplateParamField(body, index, ['description'])
                  : setTemplateParamField(body, index, ['description'], next),
              )
            }
          />
        </FieldChrome>
      </div>
    </li>
  )
}

/** First unused `parameter`, `parameter_2`, … for a freshly added row. */
function nextParamName(params: TemplateParamRead[]): string {
  const taken = new Set(params.map((p) => p.name))
  if (!taken.has('parameter')) return 'parameter'
  for (let n = 2; ; n++) {
    const candidate = `parameter_${n}`
    if (!taken.has(candidate)) return candidate
  }
}

/** Render a stored default value as editable text (YAML/JSON flow for non-strings). */
function formatDefault(value: unknown): string {
  if (value === undefined) return ''
  if (typeof value === 'string') return value
  try {
    return JSON.stringify(value)
  } catch {
    return String(value)
  }
}

/**
 * Interpret the default input back to a YAML value. A declared `string` type
 * is stored verbatim; otherwise the text is parsed as YAML (so `{}`, `[]`,
 * `42`, `true` round-trip), falling back to the raw string when it will not
 * parse. The value round-trips through the comment-preserving writer.
 */
function parseDefault(text: string, type: string | undefined): unknown {
  if (type === 'string') return text
  try {
    return parseYaml(text)
  } catch {
    return text
  }
}
