import { useRef, useEffect } from 'react'

interface GraphSearchInputProps {
  query: string
  onQueryChange: (q: string) => void
  onClear: () => void
  matchCount: number
  totalCount: number
  isSearchActive: boolean
  onFitToMatches?: () => void
  placeholder?: string
}

export function GraphSearchInput({
  query,
  onQueryChange,
  onClear,
  matchCount,
  totalCount,
  isSearchActive,
  onFitToMatches,
  placeholder = 'Search nodes...',
}: GraphSearchInputProps) {
  const inputRef = useRef<HTMLInputElement>(null)

  // Cmd/Ctrl+F focuses the search input
  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      if ((e.metaKey || e.ctrlKey) && e.key === 'f') {
        e.preventDefault()
        inputRef.current?.focus()
      }
    }
    document.addEventListener('keydown', handler)
    return () => document.removeEventListener('keydown', handler)
  }, [])

  return (
    <div className="flex items-center gap-2">
      <div className="relative">
        {/* Search icon */}
        <svg
          className="absolute left-2 top-1/2 h-3.5 w-3.5 -translate-y-1/2 text-slate-400"
          fill="none"
          viewBox="0 0 24 24"
          stroke="currentColor"
        >
          <path
            strokeLinecap="round"
            strokeLinejoin="round"
            strokeWidth={2}
            d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z"
          />
        </svg>

        <input
          ref={inputRef}
          type="text"
          placeholder={placeholder}
          value={query}
          onChange={(e) => onQueryChange(e.target.value)}
          onKeyDown={(e) => {
            if (e.key === 'Escape') {
              onClear()
              inputRef.current?.blur()
            }
          }}
          className="rounded border border-slate-200 py-1 pl-7 pr-7 text-xs text-slate-700 placeholder-slate-400 focus:border-blue-400 focus:outline-none"
          style={{ width: 180 }}
        />

        {/* Clear button */}
        {isSearchActive && (
          <button
            onClick={onClear}
            className="absolute right-1.5 top-1/2 -translate-y-1/2 rounded p-0.5 text-slate-400 hover:text-slate-600"
          >
            <svg className="h-3 w-3" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
            </svg>
          </button>
        )}
      </div>

      {/* Match count badge */}
      {isSearchActive && (
        <span className="whitespace-nowrap text-[11px] text-slate-400">
          {matchCount}/{totalCount}
        </span>
      )}

      {/* Zoom to matches button */}
      {isSearchActive && matchCount > 0 && onFitToMatches && (
        <button
          onClick={onFitToMatches}
          title="Zoom to matches"
          className="rounded p-1 text-slate-400 hover:bg-slate-100 hover:text-slate-600"
        >
          <svg className="h-3.5 w-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path
              strokeLinecap="round"
              strokeLinejoin="round"
              strokeWidth={2}
              d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0zM10 7v3m0 0v3m0-3h3m-3 0H7"
            />
          </svg>
        </button>
      )}
    </div>
  )
}
