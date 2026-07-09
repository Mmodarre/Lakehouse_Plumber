import { Check, CircleX, Loader2, Minus, Wrench } from 'lucide-react'
import { cn } from '../../lib/utils'
import { useHealth } from '../../hooks/useProject'
import type { AssistantItem } from '../../types/assistant'
import { parseToolArguments } from './toolDisplay'
import { getToolRenderer } from './toolRenderers'

// Shared conversation-item renderer: the SAME component serves live
// `item.started`/`item.done` frames and normalized session-snapshot items
// (which arrive enveloped and are unwrapped by `normalizeSnapshotItem`
// before reaching here) — one renderer path, per spike S8.

function StatusIcon({ status }: { status: string | null }) {
  if (status === 'running') {
    return (
      <Loader2
        className="size-3.5 shrink-0 animate-spin text-muted-foreground"
        aria-label="running"
      />
    )
  }
  if (status === 'incomplete') {
    return (
      <Minus
        className="size-3.5 shrink-0 text-muted-foreground"
        aria-label="incomplete"
      />
    )
  }
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
  const renderer = getToolRenderer(name)
  const { title, subtitle } = renderer.display(args, health?.root)
  const output =
    typeof item.output_preview === 'string' && item.output_preview !== ''
      ? item.output_preview
      : null

  // In-flight and abandoned calls stay header-only; the body renders once
  // the call is done (or immediately for unknown tools' raw JSON).
  const settled = status !== 'running' && status !== 'incomplete'
  const body = renderer.Body ? (
    renderer.Body({ args, output, root: health?.root })
  ) : renderer.unknown && Object.keys(args).length > 0 ? (
    <pre className="mt-1 max-h-48 overflow-auto rounded bg-muted p-2 text-2xs text-foreground">
      {JSON.stringify(args, null, 2)}
    </pre>
  ) : null
  const showOutput = renderer.Body === undefined && output !== null

  return (
    <div
      className={cn(
        'rounded-md border border-border bg-card px-2.5 py-1.5',
        status === 'incomplete' && 'border-dashed opacity-60',
      )}
    >
      <div className="flex items-center gap-1.5" title={status ?? undefined}>
        <Wrench
          className="size-3.5 shrink-0 text-muted-foreground"
          aria-hidden="true"
        />
        <span className="shrink-0 text-xs font-medium text-foreground">{title}</span>
        {subtitle != null && subtitle !== '' && (
          <span className="min-w-0 flex-1 truncate font-mono text-2xs text-muted-foreground">
            {subtitle}
          </span>
        )}
        <span className="ml-auto flex shrink-0 items-center">
          <StatusIcon status={status} />
        </span>
      </div>
      {settled && (body !== null || showOutput) && (
        <details className="mt-1">
          <summary className="cursor-pointer text-2xs text-muted-foreground select-none">
            Details
          </summary>
          {body}
          {showOutput && (
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
