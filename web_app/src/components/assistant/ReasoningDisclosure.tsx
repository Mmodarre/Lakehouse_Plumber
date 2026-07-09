import { Brain } from 'lucide-react'

/** Assistant reasoning text, collapsed by default behind a disclosure. */
export function ReasoningDisclosure({ text }: { text: string }) {
  return (
    <details className="rounded-md border border-border bg-muted/40 px-2.5 py-1.5">
      <summary className="flex cursor-pointer items-center gap-1.5 text-xs font-medium text-muted-foreground select-none">
        <Brain className="size-3.5 shrink-0" aria-hidden="true" />
        Reasoning
      </summary>
      <div className="mt-1.5 text-xs break-words whitespace-pre-wrap text-muted-foreground">
        {text}
      </div>
    </details>
  )
}
