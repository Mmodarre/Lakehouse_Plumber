import { Wrench } from 'lucide-react'
import type { MessagePart } from '../../store/assistantConversation'
import { ToolCallCard } from './ToolCallCard'
import { getToolRenderer } from './toolRenderers'

/** "Read ×3, Search ×2, Command ×2" in first-appearance order. */
function summarize(parts: MessagePart[]): string {
  const counts = new Map<string, number>()
  for (const part of parts) {
    if (part.kind !== 'item') continue
    const name =
      (typeof part.item.name === 'string' && part.item.name) ||
      (typeof part.item.type === 'string' && part.item.type) ||
      'item'
    const label = getToolRenderer(name).label
    counts.set(label, (counts.get(label) ?? 0) + 1)
  }
  return Array.from(counts, ([label, n]) =>
    n === 1 ? label : `${label} ×${n}`,
  ).join(', ')
}

/** One collapsed run of completed tool calls (see `groupParts`). */
export function ToolCallGroup({ parts }: { parts: MessagePart[] }) {
  return (
    <details className="rounded-md border border-border bg-card px-2.5 py-1.5">
      <summary className="flex cursor-pointer items-center gap-1.5 text-xs select-none">
        <Wrench
          className="size-3.5 shrink-0 text-muted-foreground"
          aria-hidden="true"
        />
        <span className="shrink-0 font-medium text-foreground">
          {parts.length} tool calls
        </span>
        <span className="min-w-0 flex-1 truncate text-2xs text-muted-foreground">
          · {summarize(parts)}
        </span>
      </summary>
      <div className="mt-1.5 flex flex-col gap-1.5">
        {parts.map((part) =>
          part.kind === 'item' ? (
            <ToolCallCard key={part.id} item={part.item} />
          ) : null,
        )}
      </div>
    </details>
  )
}
