import type { LucideIcon } from 'lucide-react'
import type { ReactNode } from 'react'
import { cn } from '../../../lib/utils'

/** Hidden handle styling shared by every custom node. */
export const NODE_HANDLE_CLASS = '!h-1 !w-1 !border-0 !bg-transparent'

const MAX_LABEL_CHARS = 34

// Middle-ellipsis keeps both the catalog prefix and the table segment of
// dense names visible; the full label lives in the card's title attribute.
function middleEllipsis(text: string): string {
  if (text.length <= MAX_LABEL_CHARS) return text
  const half = Math.floor((MAX_LABEL_CHARS - 1) / 2)
  return `${text.slice(0, half)}…${text.slice(-half)}`
}

// Substitution tokens (`${...}`) and fully-qualified names render in mono.
function isTokenized(label: string): boolean {
  return label.includes('${') || label.includes('.')
}

interface NodeCardProps {
  label: string
  sublabel: string
  icon: LucideIcon
  /** Identity/kind tint for the icon chip, e.g. "bg-node-pipeline/12 text-node-pipeline". */
  chipClassName: string
  selected?: boolean
  searchMatch?: boolean
  searchDimmed?: boolean
  /** Dashed border marks external sources. */
  dashed?: boolean
  /** De-emphasized label (external sources). */
  muted?: boolean
  /** Per-node-type width constraints; defaults to min-w-50 / max-w-75. */
  className?: string
  children?: ReactNode
}

export function NodeCard({
  label,
  sublabel,
  icon: Icon,
  chipClassName,
  selected,
  searchMatch,
  searchDimmed,
  dashed,
  muted,
  className,
  children,
}: NodeCardProps) {
  return (
    <div
      title={label}
      className={cn(
        'relative flex min-w-50 max-w-75 items-center gap-2.5 rounded-lg border bg-card px-3 py-2 shadow-xs',
        'transition-[opacity,border-color,box-shadow] duration-200',
        searchMatch
          ? 'border-ring shadow-md ring-2 ring-ring/30'
          : selected
            ? 'border-ring ring-2 ring-ring/20'
            : 'border-border hover:border-ring/40 hover:shadow-sm',
        dashed && 'border-dashed',
        searchDimmed && 'opacity-30',
        className,
      )}
    >
      <span
        className={cn(
          'flex size-6 shrink-0 items-center justify-center rounded-md',
          chipClassName,
        )}
      >
        <Icon className="size-3.5" aria-hidden="true" />
      </span>
      <div className="min-w-0 flex-1">
        <div
          className={cn(
            'truncate font-medium',
            muted ? 'text-muted-foreground' : 'text-card-foreground',
            isTokenized(label) ? 'font-mono text-xs' : 'text-sm',
          )}
        >
          {middleEllipsis(label)}
        </div>
        <div className="truncate text-2xs text-muted-foreground">{sublabel}</div>
      </div>
      {children}
    </div>
  )
}
