import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select'
import { SegmentedControl } from '@/components/ui/segmented-control'
import type { SchemaPath } from '@/lib/schema-help'
import { FieldChrome } from './FieldChrome'
import { issueId } from './fieldSupport'

// ── EnumSelect — optional enum key ───────────────────────────
//
// Select over a fixed set of allowed values. When `unsetLabel` is given,
// an extra entry represents "key absent": choosing it DELETES the key
// (pristine absence), and it is what shows while the key is not in the
// file. A present value outside `options` renders as a placeholder; the
// caller's validator supplies the `issue` text for it.

/** Sentinel item value for the delete-the-key entry (never a real option). */
const UNSET = '\u0000unset'

export interface EnumSelectProps {
  /** DOM id (label + issue wiring). */
  id: string
  label: string
  /** Current YAML value; `undefined` = key absent. */
  value: string | undefined
  /** Allowed values, in display order. */
  options: readonly string[]
  /** Label of the "not set" entry (e.g. `Not set (default: table)`). Omit to require a value. */
  unsetLabel?: string
  /** Set the key to one of `options`. */
  onSet: (value: string) => void
  /** Delete the key (invoked via the `unsetLabel` entry). */
  onUnset?: () => void
  /** Schema path the (i) tooltip resolves help from. */
  helpPath?: SchemaPath
  /** Explicit help override; wins over helpPath. */
  help?: string
  description?: string
  /** Validation message shown under the field. */
  issue?: string
  disabled?: boolean
  /** Control style. `'segmented'` renders a SegmentedControl; default is the dropdown. */
  display?: 'segmented' | 'select'
}

export function EnumSelect({
  id,
  label,
  value,
  options,
  unsetLabel,
  onSet,
  onUnset,
  helpPath,
  help,
  description,
  issue,
  disabled,
  display,
}: EnumSelectProps) {
  const known = value !== undefined && options.includes(value)
  const selectValue = known ? value : value === undefined && unsetLabel ? UNSET : undefined

  if (display === 'segmented') {
    return (
      <FieldChrome
        id={id}
        label={label}
        helpPath={helpPath}
        help={help}
        description={description}
        issue={issue}
      >
        <SegmentedControl
          size="sm"
          aria-label={label}
          // Radix shows nothing selected for an unknown/absent value; there is
          // no synthetic unset segment (a segmented control is non-deselectable).
          value={known && value !== undefined ? value : ''}
          // Same value path as the Select branch: hand the chosen option string
          // to `onSet` (SegmentedControl only forwards concrete option values,
          // never UNSET, so no unset routing is needed here).
          onValueChange={onSet}
          options={options.map((option) => ({ value: String(option), label: option, disabled }))}
        />
      </FieldChrome>
    )
  }

  return (
    <FieldChrome
      id={id}
      label={label}
      helpPath={helpPath}
      help={help}
      description={description}
      issue={issue}
    >
      <Select
        value={selectValue}
        onValueChange={(next) => {
          if (next === UNSET) onUnset?.()
          else onSet(next)
        }}
        disabled={disabled}
      >
        <SelectTrigger
          id={id}
          size="sm"
          className="w-full font-mono text-xs"
          aria-invalid={issue !== undefined ? true : undefined}
          aria-describedby={issueId(id)}
        >
          <SelectValue placeholder={value !== undefined ? String(value) : 'Select…'} />
        </SelectTrigger>
        <SelectContent>
          {unsetLabel && (
            <SelectItem value={UNSET} className="text-xs">
              {unsetLabel}
            </SelectItem>
          )}
          {options.map((option) => (
            <SelectItem key={option} value={option} className="font-mono text-xs">
              {option}
            </SelectItem>
          ))}
        </SelectContent>
      </Select>
    </FieldChrome>
  )
}
