import { useMemo, useState } from 'react'
import { Search, X } from 'lucide-react'
import { cn } from '@/lib/utils'
import type { ActionKind } from '@/lib/flowgroup-doc'
import { Dialog, DialogContent, DialogTitle } from '@/components/ui/dialog'
import { Input } from '@/components/ui/input'
import { Button } from '@/components/ui/button'
import { listActionSpecs } from './specs/registry'

// ── ActionPalette — pick an action kind → sub-type ───────────
//
// The single add-action surface, opened from the empty-state CTA, the
// top-bar "Add action", and every node's "+". Sub-types come straight from
// the spec registry (each spec carries its own title + summary), grouped by
// kind and railed in the kind's accent colour — the same four-colour language
// the inspector and nodes use. Picking one hands (kind, subType) back to the
// canvas, which builds the skeleton and writes it through.

const KINDS: { kind: ActionKind; label: string }[] = [
  { kind: 'load', label: 'Load' },
  { kind: 'transform', label: 'Transform' },
  { kind: 'write', label: 'Write' },
  { kind: 'test', label: 'Test' },
]

const KIND_RAIL: Record<ActionKind, string> = {
  load: 'bg-kind-load',
  transform: 'bg-kind-transform',
  write: 'bg-kind-write',
  test: 'bg-kind-test',
}

export interface ActionPaletteProps {
  open: boolean
  /** Contextual title, e.g. "Add action" or "Add downstream of load_orders". */
  title: string
  /** Optional one-line context (the view the new action will read). */
  subtitle?: string
  onClose: () => void
  onPick: (kind: ActionKind, subType: string) => void
}

export function ActionPalette({ open, title, subtitle, onClose, onPick }: ActionPaletteProps) {
  // Unmount while closed so the search field resets on each open.
  if (!open) return null
  return (
    <Dialog
      open
      onOpenChange={(o) => {
        if (!o) onClose()
      }}
    >
      <DialogContent
        showCloseButton={false}
        aria-describedby={undefined}
        className="flex max-h-[80vh] flex-col gap-0 overflow-hidden p-0 sm:max-w-lg"
      >
        <PaletteBody title={title} subtitle={subtitle} onClose={onClose} onPick={onPick} />
      </DialogContent>
    </Dialog>
  )
}

function PaletteBody({
  title,
  subtitle,
  onClose,
  onPick,
}: Omit<ActionPaletteProps, 'open'>) {
  const [query, setQuery] = useState('')
  const specs = listActionSpecs()

  const filtered = useMemo(() => {
    const q = query.trim().toLowerCase()
    if (q === '') return specs
    return specs.filter((s) =>
      `${s.kind} ${s.subType} ${s.title} ${s.summary ?? ''}`.toLowerCase().includes(q),
    )
  }, [specs, query])

  const pick = (kind: ActionKind, subType: string) => {
    onPick(kind, subType)
    onClose()
  }

  return (
    <>
      <div className="flex items-start justify-between border-b border-border px-5 py-3">
        <div className="min-w-0">
          <span className="text-2xs font-semibold uppercase tracking-[0.05em] text-muted-foreground">
            Add action
          </span>
          <DialogTitle className="truncate text-sm font-semibold text-foreground">
            {title}
          </DialogTitle>
          {subtitle && <p className="mt-0.5 truncate text-2xs text-muted-foreground">{subtitle}</p>}
        </div>
        <Button
          variant="ghost"
          size="icon-sm"
          className="ml-3 shrink-0 text-muted-foreground"
          aria-label="Close"
          onClick={onClose}
        >
          <X aria-hidden="true" />
        </Button>
      </div>

      <div className="border-b border-border px-5 py-2.5">
        <div className="relative">
          <Search
            className="pointer-events-none absolute left-2 top-1/2 size-3.5 -translate-y-1/2 text-muted-foreground"
            aria-hidden="true"
          />
          <Input
            autoFocus
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            placeholder="Filter action types…"
            aria-label="Filter action types"
            className="h-8 pl-7 text-xs"
          />
        </div>
      </div>

      <div className="min-h-0 flex-1 overflow-y-auto px-3 py-2">
        {filtered.length === 0 ? (
          <p className="px-2 py-6 text-center text-xs text-muted-foreground">
            No action types match “{query}”.
          </p>
        ) : (
          KINDS.map(({ kind, label }) => {
            const rows = filtered.filter((s) => s.kind === kind)
            if (rows.length === 0) return null
            return (
              <section key={kind} className="mb-2">
                <div className="flex items-center gap-2 px-2 py-1">
                  <span className={cn('h-3 w-0.5 rounded-full', KIND_RAIL[kind])} aria-hidden="true" />
                  <span className="text-2xs font-semibold uppercase tracking-[0.05em] text-muted-foreground">
                    {label}
                  </span>
                </div>
                {rows.map((spec) => (
                  <button
                    key={`${spec.kind}/${spec.subType}`}
                    type="button"
                    onClick={() => pick(spec.kind, spec.subType)}
                    className="group flex w-full items-start gap-2 rounded-sm px-2 py-1.5 text-left transition-colors hover:bg-muted/60 focus-visible:bg-muted/60 focus-visible:outline-none"
                  >
                    <span
                      className={cn(
                        'mt-1 h-3 w-0.5 shrink-0 rounded-full opacity-60 group-hover:opacity-100',
                        KIND_RAIL[kind],
                      )}
                      aria-hidden="true"
                    />
                    <span className="min-w-0 flex-1">
                      <span className="flex items-baseline gap-1.5">
                        <span className="truncate text-xs font-medium text-foreground">
                          {spec.title}
                        </span>
                        <span className="shrink-0 font-mono text-2xs text-muted-foreground">
                          {spec.subType}
                        </span>
                      </span>
                      {spec.summary && (
                        <span className="mt-0.5 block text-2xs text-muted-foreground">
                          {spec.summary}
                        </span>
                      )}
                    </span>
                  </button>
                ))}
              </section>
            )
          })
        )}
      </div>
    </>
  )
}
