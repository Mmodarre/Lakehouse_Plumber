import { useRef, useEffect } from 'react'
import { ScanSearch, Search, X } from 'lucide-react'
import { Button } from '../ui/button'
import { Input } from '../ui/input'

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
        <Search
          className="pointer-events-none absolute top-1/2 left-2 size-3.5 -translate-y-1/2 text-muted-foreground"
          aria-hidden="true"
        />

        <Input
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
          className="h-7 w-45 pr-7 pl-7 text-xs md:text-xs"
        />

        {isSearchActive && (
          <button
            type="button"
            onClick={onClear}
            aria-label="Clear search"
            className="absolute top-1/2 right-1.5 -translate-y-1/2 rounded-sm p-0.5 text-muted-foreground transition-colors duration-150 hover:text-foreground"
          >
            <X className="size-3" aria-hidden="true" />
          </button>
        )}
      </div>

      {isSearchActive && (
        <span className="text-2xs whitespace-nowrap text-muted-foreground tabular-nums">
          {matchCount}/{totalCount}
        </span>
      )}

      {isSearchActive && matchCount > 0 && onFitToMatches && (
        <Button
          variant="ghost"
          size="icon-xs"
          onClick={onFitToMatches}
          title="Zoom to matches"
          aria-label="Zoom to matches"
        >
          <ScanSearch aria-hidden="true" />
        </Button>
      )}
    </div>
  )
}
