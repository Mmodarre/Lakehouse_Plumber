import { Info } from 'lucide-react'
import { Tooltip, TooltipContent, TooltipTrigger } from '@/components/ui/tooltip'
import { cn } from '@/lib/utils'

export interface FieldHelpProps {
  /** Already-resolved help text; when falsy the icon is not rendered. */
  text?: string
  /** Field label, used to build the button's accessible name. */
  label?: string
  side?: 'top' | 'right' | 'bottom' | 'left'
  className?: string
}

/** Focusable "(i)" info icon that reveals help text in a tooltip on hover or
 * focus. Presentational only: it takes resolved `text` and does NOT resolve
 * schema help itself (FieldLabel owns the `useFieldHelp` call). A real
 * `<button>` trigger gives both hover- and focus-open for free via Radix.
 * Renders nothing when there is no help text. */
export function FieldHelp({ text, label, side, className }: FieldHelpProps) {
  if (!text) return null
  return (
    <Tooltip>
      <TooltipTrigger asChild>
        <button
          type="button"
          aria-label={label ? `More info about ${label}` : 'More info'}
          className={cn(
            'inline-flex rounded-sm text-muted-foreground outline-none transition-colors hover:text-foreground focus-visible:text-foreground focus-visible:ring-[3px] focus-visible:ring-ring/50',
            className,
          )}
        >
          <Info className="size-3.5" />
        </button>
      </TooltipTrigger>
      <TooltipContent className="max-w-xs" side={side}>
        {text}
      </TooltipContent>
    </Tooltip>
  )
}
