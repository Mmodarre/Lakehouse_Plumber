import { TriangleAlert } from 'lucide-react'
import { useCircularDeps } from '../../hooks/useDependencyGraph'
import { GraphSearchInput } from './GraphSearchInput'

interface GraphControlsProps {
  query: string
  onQueryChange: (q: string) => void
  onClear: () => void
  matchCount: number
  totalCount: number
  isSearchActive: boolean
  onFitToMatches?: () => void
  placeholder?: string
}

export function GraphControls({
  query,
  onQueryChange,
  onClear,
  matchCount,
  totalCount,
  isSearchActive,
  onFitToMatches,
  placeholder,
}: GraphControlsProps) {
  const { data: circular } = useCircularDeps()

  return (
    <div className="flex items-center gap-3 border-b border-border bg-card px-4 py-1.5">
      <GraphSearchInput
        query={query}
        onQueryChange={onQueryChange}
        onClear={onClear}
        matchCount={matchCount}
        totalCount={totalCount}
        isSearchActive={isSearchActive}
        onFitToMatches={onFitToMatches}
        placeholder={placeholder}
      />

      <div className="flex-1" />

      {circular?.has_circular && (
        <div className="flex items-center gap-1.5 rounded-md border border-warning/25 bg-warning/10 px-2 py-1 text-2xs text-foreground">
          <TriangleAlert className="size-3.5 shrink-0 text-warning" aria-hidden="true" />
          <span>{circular.total_cycles} circular dep{circular.total_cycles > 1 ? 's' : ''}</span>
        </div>
      )}
    </div>
  )
}
