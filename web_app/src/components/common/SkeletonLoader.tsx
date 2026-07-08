import { Skeleton } from '@/components/ui/skeleton'

/** Deterministic per-line widths in the 70–100% band. Replaces a former
 * `Math.random()` call during render (impure) — the bars still vary in length
 * but no longer jitter on every re-render, and the render is now pure. */
function skeletonWidth(index: number): string {
  // A small fixed pseudo-random sequence keyed by line index.
  const fractions = [0.18, 0.74, 0.42, 0.93, 0.06, 0.61, 0.31, 0.85]
  const fraction = fractions[index % fractions.length]
  return `${70 + fraction * 30}%`
}

export function SkeletonLoader({ lines = 3 }: { lines?: number }) {
  return (
    <div className="space-y-2 p-3" aria-hidden="true">
      {Array.from({ length: lines }).map((_, i) => (
        <Skeleton key={i} className="h-3" style={{ width: skeletonWidth(i) }} />
      ))}
    </div>
  )
}

/** Table-shaped placeholder: `rows` 36px skeleton rows matching the real data
 * grid, so the layout doesn't jump when data lands. Preferred over a centered
 * spinner for table/list loading. */
export function TableSkeleton({ rows = 8 }: { rows?: number }) {
  return (
    <div className="flex-1 overflow-hidden" aria-hidden="true">
      <div className="flex h-9 items-center gap-4 border-b border-border px-3">
        <Skeleton className="h-3 w-24" />
        <Skeleton className="h-3 w-16" />
        <Skeleton className="h-3 w-20" />
      </div>
      {Array.from({ length: rows }).map((_, i) => (
        <div
          key={i}
          className="flex h-9 items-center gap-4 border-b border-border/60 px-3"
        >
          <Skeleton
            className="h-3"
            style={{ width: `calc(${skeletonWidth(i)} / 4)` }}
          />
          <Skeleton className="h-5 w-14 rounded-sm" />
          <Skeleton className="h-3 w-28" />
        </div>
      ))}
    </div>
  )
}
