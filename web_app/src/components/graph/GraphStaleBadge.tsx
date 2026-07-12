import { RefreshCw } from 'lucide-react'
import { useGraphStaleness } from '../../hooks/useGraphStaleness'
import { Button } from '../ui/button'
import { cn } from '../../lib/utils'

// "Graph out of date — Refresh" affordance for the graph toolbars. Renders
// nothing while the graph is fresh, so it can be dropped into any header
// unconditionally. Clicking Refresh rebuilds the graph server-side, refetches
// the open graph views, and clears the stale flag (see useGraphStaleness).
export function GraphStaleBadge() {
  const { isStale, refresh, isRefreshing } = useGraphStaleness()

  if (!isStale && !isRefreshing) return null

  return (
    <div className="flex items-center gap-1.5 rounded-md border border-warning/25 bg-warning/10 px-2 py-1 text-2xs text-foreground">
      <span>Graph out of date</span>
      <Button
        size="xs"
        variant="outline"
        onClick={() => refresh()}
        disabled={isRefreshing}
      >
        <RefreshCw
          className={cn('size-3', isRefreshing && 'animate-spin')}
          aria-hidden="true"
        />
        {isRefreshing ? 'Refreshing…' : 'Refresh'}
      </Button>
    </div>
  )
}
