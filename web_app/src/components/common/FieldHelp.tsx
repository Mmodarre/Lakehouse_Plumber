import { lazy, Suspense, useId, useState } from 'react'
import { Info } from 'lucide-react'
import { Popover, PopoverContent, PopoverTrigger } from '@/components/ui/popover'
import type { ResolvedHelp } from '@/lib/field-help'
import { cn } from '@/lib/utils'

const FieldHelpBody = lazy(() => import('./FieldHelpBody'))
export interface FieldHelpProps {
  text?: string
  help?: ResolvedHelp
  label?: string
  side?: 'top' | 'right' | 'bottom' | 'left'
  className?: string
}

/** Deliberately opened, persistent help. Mutation controls use the read-only
 * context independently, so this native button remains available in viewer mode. */
export function FieldHelp({ text, help, label, side, className }: FieldHelpProps) {
  const titleId = useId()
  const [open, setOpen] = useState(false)
  const content = help ?? (text ? { summary: text } : undefined)
  if (!content?.summary) return null
  const triggerClass = cn('inline-flex size-7 shrink-0 cursor-pointer items-center justify-center rounded-sm text-muted-foreground outline-none transition-colors hover:text-foreground focus-visible:text-foreground focus-visible:ring-[3px] focus-visible:ring-ring/50', className)
  const name = label ? `More info about ${label}` : 'More info'
  return (
    <Popover open={open} onOpenChange={setOpen}>
      <PopoverTrigger asChild>
        <button type="button" aria-label={name} className={triggerClass}>
          <Info aria-hidden="true" className="size-3.5" />
        </button>
      </PopoverTrigger>
      <PopoverContent side={side} align="start" aria-labelledby={titleId}
        className="z-[80] max-h-[min(32rem,80dvh)] w-[min(24rem,calc(100vw-2rem))] overflow-y-auto text-sm">
        <div className="mb-3 flex items-start justify-between gap-3">
          <h2 id={titleId} className="font-semibold">{label ?? 'Field guidance'}</h2>
          <button type="button" onClick={() => setOpen(false)} className="shrink-0 rounded px-2 py-1 text-xs text-muted-foreground hover:text-foreground focus-visible:outline-2" aria-label="Close field help">Close</button>
        </div>
        <p>{content.summary}</p>
        <Suspense fallback={<p className="mt-3 text-xs text-muted-foreground" role="status">Loading guidance…</p>}>
          <FieldHelpBody help={content} />
        </Suspense>
      </PopoverContent>
    </Popover>
  )
}
