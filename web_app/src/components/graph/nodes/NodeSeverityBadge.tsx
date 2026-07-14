import { cn } from '../../../lib/utils'

// Optional validation-severity dot for a graph node, fed by useMapEnrichment's
// client-side join (§6.7 G2). Renders nothing when a node carries no severity,
// so nodes without enrichment look exactly as before.
export function NodeSeverityBadge({ severity }: { severity?: unknown }) {
  if (severity !== 'error' && severity !== 'warning') return null
  return (
    <span
      title={severity}
      aria-label={`${severity} severity`}
      className={cn(
        'size-2 shrink-0 rounded-full',
        severity === 'error' ? 'bg-error' : 'bg-warning',
      )}
    />
  )
}
