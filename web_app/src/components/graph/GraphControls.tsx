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
  onCreateFlowgroup?: () => void
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
  onCreateFlowgroup,
  placeholder,
}: GraphControlsProps) {
  const { data: circular } = useCircularDeps()

  return (
    <div className="flex items-center gap-3 border-b border-slate-100 bg-white px-4 py-1.5">
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

      {onCreateFlowgroup && (
        <button
          onClick={onCreateFlowgroup}
          title="Create new flowgroup"
          className="flex h-7 items-center gap-1.5 rounded-md bg-blue-600 px-3 text-xs font-medium text-white shadow-sm hover:bg-blue-700 active:bg-blue-800"
        >
          <svg className="h-3.5 w-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2.5} d="M12 4v16m8-8H4" />
          </svg>
          New Flowgroup
        </button>
      )}

      <div className="flex-1" />

      {circular?.has_circular && (
        <div className="flex items-center gap-1 rounded bg-amber-50 px-2 py-1 text-[11px] text-amber-700">
          <svg className="h-3.5 w-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2}
              d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-2.5L13.732 4.5c-.77-.833-2.694-.833-3.464 0L3.34 16.5c-.77.833.192 2.5 1.732 2.5z" />
          </svg>
          <span>{circular.total_cycles} circular dep{circular.total_cycles > 1 ? 's' : ''}</span>
        </div>
      )}
    </div>
  )
}
