import { Badge } from '@/components/ui/badge'
import type { SchemaPath } from '@/lib/schema-help'
import { DraftInput } from '@/components/config/fields/DraftInput'
import { FieldChrome } from '@/components/config/fields/FieldChrome'
import { issueId } from '@/components/config/fields/fieldSupport'
import { isPureTemplateParam, templateParamNames } from './specs/helpers'

// ── TokenField — a typed field currently holding a token ─────
//
// A number/bool/enum key may legitimately hold a token instead of a literal.
// Those widgets can't represent one, so the engine swaps in this field: a
// monospace text input (commit on blur/Enter, empty = delete the key) with a
// badge, so the value round-trips unchanged and stays obviously a token.
//
// Two hues encode WHO fills the value in, a real distinction when authoring:
//  - 'substitution' (amber): `${env}` / `${secret:…}` / `%{var}`, resolved at
//    generate/deploy time.
//  - 'param' (primary): a `{{ jinja }}` template parameter, filled in by
//    whoever instantiates the template. A pure `{{ x }}` reads as bound to x.

export interface TokenFieldProps {
  id: string
  label: string
  value: string
  onSet: (value: string) => void
  onUnset: () => void
  /** Schema path the (i) tooltip resolves help from. */
  helpPath?: SchemaPath
  /** Explicit help override; wins over helpPath. */
  help?: string
  disabled?: boolean
  /** Token hue/semantics — see the module header. Default 'substitution'. */
  variant?: 'substitution' | 'param'
}

export function TokenField({
  id,
  label,
  value,
  onSet,
  onUnset,
  helpPath,
  help,
  disabled,
  variant = 'substitution',
}: TokenFieldProps) {
  const param = variant === 'param'
  const names = param ? templateParamNames(value) : []
  const badgeLabel = param ? (isPureTemplateParam(value) && names[0] ? names[0] : 'param') : 'token'
  const note = param
    ? paramNote(value, names)
    : 'Holds a substitution token — resolved at generate time.'

  return (
    <FieldChrome
      id={id}
      label={label}
      helpPath={helpPath}
      help={help}
      // Substitution keeps the amber advisory line; a template parameter is
      // normal in a template, so its note rides the neutral description slot.
      issue={param ? undefined : note}
      issueSeverity="warning"
    >
      <div className="flex items-center gap-1.5">
        <DraftInput
          id={id}
          initial={value}
          onCommit={(next) => (next === '' ? onUnset() : onSet(next))}
          monospace
          disabled={disabled}
          aria-describedby={issueId(id)}
        />
        <Badge
          variant="outline"
          className={
            param
              ? 'shrink-0 rounded-sm border-primary/50 px-1.5 font-mono text-2xs text-primary'
              : 'shrink-0 rounded-sm border-warning/50 px-1.5 text-2xs text-warning'
          }
        >
          {badgeLabel}
        </Badge>
      </div>
      {param && <p className="text-2xs text-muted-foreground">{note}</p>}
    </FieldChrome>
  )
}

/** "Bound to parameter x." for a pure ref, else the list it references. */
function paramNote(value: string, names: string[]): string {
  if (isPureTemplateParam(value) && names.length === 1) {
    return `Bound to template parameter ${names[0]}.`
  }
  if (names.length > 0) {
    return `References template parameter${names.length > 1 ? 's' : ''}: ${names.join(', ')}.`
  }
  return 'Template parameter reference — filled in when the template is used.'
}
