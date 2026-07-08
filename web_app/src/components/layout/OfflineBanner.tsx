import { TriangleAlert } from 'lucide-react'
import { Button } from '../ui/button'

/**
 * Slim banner shown under the header when the health poll errors, signalling
 * that the SPA cannot reach the LHP backend. "Retry" re-runs the health query.
 */
export function OfflineBanner({ onRetry }: { onRetry: () => void }) {
  return (
    <div
      role="alert"
      className="flex shrink-0 items-center justify-between gap-3 border-b border-error/25 bg-error/10 px-4 py-1.5 text-xs text-error"
    >
      <span className="inline-flex min-w-0 items-center gap-1.5">
        <TriangleAlert className="size-3.5 shrink-0" aria-hidden="true" />
        <span className="truncate">Cannot reach the LHP server. Edits may not be saved.</span>
      </span>
      <Button
        variant="outline"
        size="xs"
        onClick={onRetry}
        className="shrink-0 border-error/30 bg-transparent text-error hover:bg-error/10 hover:text-error"
      >
        Retry
      </Button>
    </div>
  )
}
