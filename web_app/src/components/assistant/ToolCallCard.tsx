import { Check, CircleX, Wrench } from 'lucide-react'
import { useHealth } from '../../hooks/useProject'
import type { AssistantItem } from '../../types/assistant'
import { parseToolArguments, toolDisplay } from './toolDisplay'

// Shared conversation-item renderer: the SAME component serves live
// `item.done` frames and normalized session-snapshot items (which arrive
// enveloped and are unwrapped by `normalizeSnapshotItem` before reaching
// here) — one renderer path, per spike S8.

function StatusIcon({ status }: { status: string | null }) {
  if (status === 'completed') {
    return <Check className="size-3.5 shrink-0 text-success" aria-label="completed" />
  }
  if (status === 'failed') {
    return <CircleX className="size-3.5 shrink-0 text-error" aria-label="failed" />
  }
  return null
}

export function ToolCallCard({ item }: { item: AssistantItem }) {
  const { data: health } = useHealth()
  const name =
    (typeof item.name === 'string' && item.name) ||
    (typeof item.type === 'string' && item.type) ||
    'item'
  const status = typeof item.status === 'string' ? item.status : null
  const args = parseToolArguments(item)
  const { label, detail } = toolDisplay(name, args, health?.root)
  const output =
    typeof item.output_preview === 'string' && item.output_preview !== ''
      ? item.output_preview
      : null

  return (
    <div className="rounded-md border border-border bg-card px-2.5 py-1.5">
      <div className="flex items-center gap-1.5" title={status ?? undefined}>
        <Wrench
          className="size-3.5 shrink-0 text-muted-foreground"
          aria-hidden="true"
        />
        <span className="shrink-0 text-xs font-medium text-foreground">{label}</span>
        {detail && (
          <span className="min-w-0 flex-1 truncate font-mono text-2xs text-muted-foreground">
            {detail}
          </span>
        )}
        <span className="ml-auto flex shrink-0 items-center">
          <StatusIcon status={status} />
        </span>
      </div>
      {(Object.keys(args).length > 0 || output) && (
        <details className="mt-1">
          <summary className="cursor-pointer text-2xs text-muted-foreground select-none">
            Details
          </summary>
          {Object.keys(args).length > 0 && (
            <pre className="mt-1 max-h-48 overflow-auto rounded bg-muted p-2 text-2xs text-foreground">
              {JSON.stringify(args, null, 2)}
            </pre>
          )}
          {output && (
            <>
              <p className="mt-1 text-2xs text-muted-foreground">Output</p>
              <pre className="mt-0.5 max-h-48 overflow-auto rounded bg-muted p-2 text-2xs text-foreground">
                {output}
              </pre>
            </>
          )}
        </details>
      )}
    </div>
  )
}
