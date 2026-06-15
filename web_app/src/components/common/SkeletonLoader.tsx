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
    <div className="animate-pulse space-y-2 p-3">
      {Array.from({ length: lines }).map((_, i) => (
        <div
          key={i}
          className="h-3 rounded bg-slate-200"
          style={{ width: skeletonWidth(i) }}
        />
      ))}
    </div>
  )
}
