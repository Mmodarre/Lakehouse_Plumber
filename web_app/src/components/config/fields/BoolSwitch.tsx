import { Button } from '@/components/ui/button'
import { Switch } from '@/components/ui/switch'
import type { SchemaPath } from '@/lib/schema-help'
import { FieldLabel } from './FieldLabel'
import { issueId } from './fieldSupport'

// ── BoolSwitch — optional boolean key with tri-state display ─
//
// Presents an OPTIONAL boolean key without ever writing a value the user
// did not ask for:
//   • key absent  → the switch shows the loader's default, subtly marked
//     "default: on/off"; flipping it SETS the key explicitly;
//   • key present → the explicit value shows, plus a "Reset to default"
//     affordance that DELETES the key (pristine absence — the file goes
//     back to not mentioning the key at all).

export interface BoolSwitchProps {
  /** DOM id (label + issue wiring). */
  id: string
  label: string
  /**
   * Parsed current value; `undefined` = key absent (or unparseable — the
   * caller's validator surfaces that via `issue`).
   */
  value: boolean | undefined
  /** The loader's default, displayed while the key is absent. */
  defaultValue: boolean
  /** Set the key to an explicit boolean. */
  onSet: (value: boolean) => void
  /** Delete the key — back to inheriting the default. */
  onReset: () => void
  /** Schema path the (i) tooltip resolves help from. */
  helpPath?: SchemaPath
  /** Explicit help override; wins over helpPath. */
  help?: string
  description?: string
  /** Validation message shown under the field. */
  issue?: string
  disabled?: boolean
}

export function BoolSwitch({
  id,
  label,
  value,
  defaultValue,
  onSet,
  onReset,
  helpPath,
  help,
  description,
  issue,
  disabled,
}: BoolSwitchProps) {
  const isSet = value !== undefined
  const effective = value ?? defaultValue
  return (
    <div className="space-y-1">
      <div className="flex items-center justify-between gap-3">
        <div className="min-w-0">
          <FieldLabel htmlFor={id} label={label} helpPath={helpPath} help={help ?? description} />
        </div>
        <div className="flex shrink-0 items-center gap-2">
          {!isSet && (
            <span className="text-2xs text-muted-foreground">
              default: {defaultValue ? 'on' : 'off'}
            </span>
          )}
          {isSet && (
            <Button
              type="button"
              variant="ghost"
              size="sm"
              className="h-6 px-1.5 text-2xs text-muted-foreground"
              onClick={onReset}
              disabled={disabled}
            >
              Reset to default
            </Button>
          )}
          <Switch
            id={id}
            size="sm"
            checked={effective}
            onCheckedChange={(checked) => onSet(checked)}
            disabled={disabled}
            aria-describedby={issueId(id)}
            aria-invalid={issue !== undefined ? true : undefined}
          />
        </div>
      </div>
      <p id={issueId(id)} role={issue ? 'alert' : undefined} className="text-2xs text-destructive">
        {issue ?? ''}
      </p>
    </div>
  )
}
