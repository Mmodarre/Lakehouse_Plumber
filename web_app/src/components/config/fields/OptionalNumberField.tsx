import { useState } from 'react'
import { Input } from '@/components/ui/input'
import type { SchemaPath } from '@/lib/schema-help'
import { FieldChrome } from './FieldChrome'
import { displayString, issueId } from './fieldSupport'

// ── OptionalNumberField — optional integer key ───────────────
//
// Commit-on-blur/Enter integer field. An empty commit DELETES the key
// (pristine absence — the loader default applies). Input that is not an
// integer, or is outside the passed bounds, is refused at commit time: a
// field-local error shows and the YAML stays untouched (nothing invalid
// is ever written through this field; pre-existing invalid values in the
// file surface via the caller's `issue`).

export interface OptionalNumberFieldProps {
  /** DOM id (label + issue wiring). */
  id: string
  label: string
  /** Current YAML value; `undefined` = key absent. */
  value: unknown
  /** Inclusive bounds enforced at commit time. */
  min?: number
  max?: number
  /** Commit a valid integer — set the key. */
  onSet: (value: number) => void
  /** Commit an empty value — DELETE the key. */
  onUnset: () => void
  /** Schema path the (i) tooltip resolves help from. */
  helpPath?: SchemaPath
  /** Explicit help override; wins over helpPath. */
  help?: string
  description?: string
  /** Shown while empty — use it to surface the loader default. */
  placeholder?: string
  /** Validation message from the caller (config-model). */
  issue?: string
  disabled?: boolean
}

export function OptionalNumberField({
  id,
  label,
  value,
  min,
  max,
  onSet,
  onUnset,
  helpPath,
  help,
  description,
  placeholder,
  issue,
  disabled,
}: OptionalNumberFieldProps) {
  const initial = displayString(value)
  const [draft, setDraft] = useState(initial)
  const [localError, setLocalError] = useState<string | null>(null)
  // Re-sync when the committed value changes — render-phase adjustment.
  const [lastInitial, setLastInitial] = useState(initial)
  if (initial !== lastInitial) {
    setLastInitial(initial)
    setDraft(initial)
    setLocalError(null)
  }

  const commit = () => {
    const trimmed = draft.trim()
    if (trimmed === initial) return
    if (trimmed === '') {
      setLocalError(null)
      onUnset()
      return
    }
    if (!/^[+-]?\d+$/.test(trimmed)) {
      setLocalError('Must be a whole number')
      return
    }
    const parsed = Number.parseInt(trimmed, 10)
    if ((min !== undefined && parsed < min) || (max !== undefined && parsed > max)) {
      setLocalError(
        min !== undefined && max !== undefined
          ? `Must be between ${min} and ${max}`
          : min !== undefined
            ? `Must be at least ${min}`
            : `Must be at most ${max}`,
      )
      return
    }
    setLocalError(null)
    onSet(parsed)
  }

  const shownIssue = localError ?? issue
  return (
    <FieldChrome
      id={id}
      label={label}
      helpPath={helpPath}
      help={help}
      description={description}
      issue={shownIssue}
    >
      <Input
        id={id}
        value={draft}
        inputMode="numeric"
        onChange={(e) => {
          setDraft(e.target.value)
          setLocalError(null)
        }}
        onBlur={commit}
        onKeyDown={(e) => {
          if (e.key === 'Enter') {
            e.preventDefault()
            commit()
          } else if (e.key === 'Escape') {
            setDraft(initial)
            setLocalError(null)
          }
        }}
        placeholder={placeholder}
        spellCheck={false}
        autoComplete="off"
        disabled={disabled}
        aria-invalid={shownIssue !== undefined && shownIssue !== null ? true : undefined}
        aria-describedby={issueId(id)}
        className="max-w-40 font-mono text-xs"
      />
    </FieldChrome>
  )
}
