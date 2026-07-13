import type { ReactNode } from 'react'
import type { SchemaPath } from '@/lib/schema-help'
import { FieldLabel } from './FieldLabel'
import { issueId } from './fieldSupport'

// ── FieldChrome — shared frame of the config field primitives ─
//
// Every field renders the same stack: <FieldLabel> (label + optional (i)
// help tooltip) + control + issue line. The issue line is ALWAYS rendered
// (empty) so `aria-describedby` has a stable target and the layout does
// not jump when a validation message appears.

export interface FieldChromeProps {
  /** DOM id of the control (Label htmlFor + issue-line id derive from it). */
  id: string
  label: string
  /** Schema path the (i) tooltip resolves help from. */
  helpPath?: SchemaPath
  /** Explicit help override; wins over helpPath. */
  help?: string
  /** Transitional help fallback fed into the tooltip until callers move to helpPath. */
  description?: string
  /** Validation message (config-model issue or field-local error). */
  issue?: string
  /** 'error' colors the issue destructive; 'warning' uses the warning token. */
  issueSeverity?: 'error' | 'warning'
  children: ReactNode
}

export function FieldChrome({
  id,
  label,
  helpPath,
  help,
  description,
  issue,
  issueSeverity = 'error',
  children,
}: FieldChromeProps) {
  return (
    <div className="space-y-1.5">
      <FieldLabel htmlFor={id} label={label} helpPath={helpPath} help={help ?? description} />
      {children}
      <p
        id={issueId(id)}
        role={issue ? 'alert' : undefined}
        className={
          issueSeverity === 'warning'
            ? 'min-h-3 text-2xs text-warning'
            : 'min-h-3 text-2xs text-destructive'
        }
      >
        {issue ?? ''}
      </p>
    </div>
  )
}
