import type { ComponentType } from 'react'
import { Plus } from 'lucide-react'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { cn } from '../../../lib/utils'

// ── Rail — the precedence rail's generic pieces ──────────────
//
// Extracted from PipelineDocList (Task 9) so the pipeline and job editors
// share one rail vocabulary: a selectable row with error/warning/duplicate
// badges, the vertical cascade connector between precedence tiers, and the
// ghost add-affordance. Tier COMPOSITION stays surface-specific
// (PipelineDocList / JobDocList).

export interface RailRowProps {
  icon: ComponentType<{ className?: string; 'aria-hidden'?: boolean | 'true' }>
  label: string
  caption?: string
  selected: boolean
  onClick: () => void
  errors?: number
  warnings?: number
  duplicate?: boolean
  /** Muted, non-mono styling (built-in tier / ignored documents). */
  ghost?: boolean
}

/** One selectable rail row. */
export function RailRow({
  icon: Icon,
  label,
  caption,
  selected,
  onClick,
  errors = 0,
  warnings = 0,
  duplicate = false,
  ghost = false,
}: RailRowProps) {
  return (
    <button
      type="button"
      onClick={onClick}
      aria-current={selected ? 'true' : undefined}
      className={cn(
        'w-full rounded-md border px-2 py-1.5 text-left transition-colors',
        selected
          ? 'border-border bg-accent'
          : 'border-transparent hover:bg-accent/50',
        ghost && 'text-muted-foreground',
      )}
    >
      <span className="flex items-center gap-1.5">
        <Icon className="size-3.5 shrink-0" aria-hidden="true" />
        <span className={cn('min-w-0 flex-1 truncate text-xs', !ghost && 'font-mono')}>
          {label}
        </span>
        {duplicate && (
          <Badge variant="destructive" className="rounded-sm px-1 text-2xs font-normal">
            duplicate
          </Badge>
        )}
        {errors > 0 && (
          <Badge
            variant="destructive"
            className="rounded-sm px-1 text-2xs font-normal"
            aria-label={`${errors} error${errors === 1 ? '' : 's'}`}
          >
            {errors}
          </Badge>
        )}
        {warnings > 0 && (
          <Badge
            variant="outline"
            className="rounded-sm border-warning/60 px-1 text-2xs font-normal text-warning"
            aria-label={`${warnings} warning${warnings === 1 ? '' : 's'}`}
          >
            {warnings}
          </Badge>
        )}
      </span>
      {caption && (
        <span className="mt-0.5 block truncate pl-5 text-2xs text-muted-foreground">
          {caption}
        </span>
      )}
    </button>
  )
}

/** Vertical cascade connector between precedence tiers. */
export function Connector() {
  return <div aria-hidden="true" className="ml-4 h-3 w-px bg-border" />
}

/** Ghost add-affordance row. */
export function AddButton({ label, onClick }: { label: string; onClick: () => void }) {
  return (
    <Button
      type="button"
      variant="ghost"
      size="sm"
      className="h-6 justify-start px-2 text-2xs text-muted-foreground"
      onClick={onClick}
    >
      <Plus aria-hidden="true" />
      {label}
    </Button>
  )
}
