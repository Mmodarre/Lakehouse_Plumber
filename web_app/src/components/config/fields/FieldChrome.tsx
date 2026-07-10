import { Label } from '@/components/ui/label'
import type { ReactNode } from 'react'
import { issueId } from './fieldSupport'

// ── FieldChrome — shared frame of the config field primitives ─
//
// Every field renders the same stack: <Label> + control + optional
// description + issue line. The issue line is ALWAYS rendered (empty) so
// `aria-describedby` has a stable target and the layout does not jump
// when a validation message appears.

export interface FieldChromeProps {
  /** DOM id of the control (Label htmlFor + issue-line id derive from it). */
  id: string
  label: string
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
  description,
  issue,
  issueSeverity = 'error',
  children,
}: FieldChromeProps) {
  return (
    <div className="space-y-1.5">
      <Label htmlFor={id} className="text-xs">
        {label}
      </Label>
      {children}
      {description && <p className="text-2xs text-muted-foreground">{description}</p>}
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
