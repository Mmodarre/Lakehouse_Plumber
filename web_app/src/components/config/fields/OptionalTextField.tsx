import type { SchemaPath } from '@/lib/schema-help'
import { DraftInput } from './DraftInput'
import { FieldChrome } from './FieldChrome'
import { displayString, issueId } from './fieldSupport'

// ── OptionalTextField — optional string key ──────────────────
//
// Commit-on-blur/Enter text field for an OPTIONAL string key: an empty
// commit means "key absent" (the caller deletes the key — pristine
// absence, never `key: ''` residue). Non-string existing values are
// displayed coerced; committing writes a proper string (the config-model
// validator flags the original type where the loader cares).

export interface OptionalTextFieldProps {
  /** DOM id (label + issue wiring). */
  id: string
  label: string
  /** Current YAML value; `undefined` = key absent. */
  value: unknown
  /** Commit a non-empty edited value — set the key. */
  onSet: (value: string) => void
  /** Commit an empty value — DELETE the key (pristine absence). */
  onUnset: () => void
  /** Schema path the (i) tooltip resolves help from. */
  helpPath?: SchemaPath
  /** Explicit help override; wins over helpPath. */
  help?: string
  description?: string
  /** Shown while empty — use it to surface the loader default. */
  placeholder?: string
  /** Mono font for values that mirror YAML/identifiers. */
  monospace?: boolean
  /** Textarea instead of a single-line input (e.g. SQL bodies). */
  multiline?: boolean
  /** Validation message shown under the field. */
  issue?: string
  issueSeverity?: 'error' | 'warning'
  disabled?: boolean
}

export function OptionalTextField({
  id,
  label,
  value,
  onSet,
  onUnset,
  helpPath,
  help,
  description,
  placeholder,
  monospace = false,
  multiline = false,
  issue,
  issueSeverity,
  disabled,
}: OptionalTextFieldProps) {
  const initial = displayString(value)
  return (
    <FieldChrome
      id={id}
      label={label}
      helpPath={helpPath}
      help={help}
      description={description}
      issue={issue}
      issueSeverity={issueSeverity}
    >
      <DraftInput
        id={id}
        initial={initial}
        onCommit={(next) => (next === '' ? onUnset() : onSet(next))}
        placeholder={placeholder}
        monospace={monospace}
        multiline={multiline}
        disabled={disabled}
        aria-invalid={issue !== undefined && issueSeverity !== 'warning' ? true : undefined}
        aria-describedby={issueId(id)}
      />
    </FieldChrome>
  )
}
