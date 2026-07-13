import type { ReactNode } from 'react'
import { Label } from '@/components/ui/label'
import { FieldHelp } from '@/components/common/FieldHelp'
import { useFieldHelp } from '@/components/common/SchemaKindContext'
import { cn } from '@/lib/utils'
import type { SchemaPath } from '@/lib/schema-help'

export interface FieldLabelProps {
  /** DOM id of the control this labels (forwarded to <Label htmlFor>). */
  htmlFor?: string
  label: ReactNode
  /** Schema path to resolve help text from. */
  helpPath?: SchemaPath
  /** Explicit help override; wins over helpPath (UI-only fields). */
  help?: string
  side?: 'top' | 'right' | 'bottom' | 'left'
  /** Class on the inline row wrapper. */
  className?: string
  /** Class on the <Label>; defaults to FieldChrome's `text-xs` so its swap
   *  in task 6 is drop-in. */
  labelClassName?: string
}

/** Label + optional focusable (i) help icon — the shared markup FieldChrome and
 * the four bypass editors adopt (task 6). This is the SINGLE place that calls
 * `useFieldHelp`; consumers just pass `helpPath`/`help` down. When there is no
 * help text, `FieldHelp` renders null and the row is just the label. */
export function FieldLabel({
  htmlFor,
  label,
  helpPath,
  help,
  side,
  className,
  labelClassName = 'text-xs',
}: FieldLabelProps) {
  const text = useFieldHelp(helpPath, help)
  return (
    <div className={cn('flex items-center gap-1', className)}>
      <Label htmlFor={htmlFor} className={labelClassName}>
        {label}
      </Label>
      <FieldHelp
        text={text}
        label={typeof label === 'string' ? label : undefined}
        side={side}
      />
    </div>
  )
}
