import { useCallback, useState } from 'react'

/** Observe when the asynchronously loaded editor actually mounts its container. */
export function useCompactConfigNavigation() {
  const [compact, setCompact] = useState(false)
  const ref = useCallback((element: HTMLDivElement | null) => {
    if (!element || typeof ResizeObserver === 'undefined') return
    const observer = new ResizeObserver(([entry]) => setCompact(entry.contentRect.width < 760))
    observer.observe(element)
    return () => observer.disconnect()
  }, [])
  return { ref, compact }
}
