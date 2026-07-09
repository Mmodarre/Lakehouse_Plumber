import { Wrench } from 'lucide-react'
import { Badge } from '../ui/badge'
import type { AssistantItem } from '../../types/assistant'

// Shared conversation-item renderer: the SAME component serves live
// `item.done` frames and normalized session-snapshot items (which arrive
// enveloped and are unwrapped by `normalizeSnapshotItem` before reaching
// here) — one renderer path, per spike S8.
export function ToolCallCard({ item }: { item: AssistantItem }) {
  const label =
    (typeof item.name === 'string' && item.name) ||
    (typeof item.type === 'string' && item.type) ||
    'item'
  const status = typeof item.status === 'string' ? item.status : null

  return (
    <div className="rounded-md border border-border bg-card px-2.5 py-1.5">
      <div className="flex items-center gap-1.5">
        <Wrench
          className="size-3.5 shrink-0 text-muted-foreground"
          aria-hidden="true"
        />
        <span className="truncate font-mono text-xs text-foreground">{label}</span>
        {status && (
          <Badge
            variant="outline"
            className="ml-auto rounded-sm px-1.5 text-2xs text-muted-foreground"
          >
            {status}
          </Badge>
        )}
      </div>
      <details className="mt-1">
        <summary className="cursor-pointer text-2xs text-muted-foreground select-none">
          Details
        </summary>
        <pre className="mt-1 max-h-48 overflow-auto rounded bg-muted p-2 text-2xs text-foreground">
          {JSON.stringify(item, null, 2)}
        </pre>
      </details>
    </div>
  )
}
