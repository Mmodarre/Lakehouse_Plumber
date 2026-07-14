import type { ReactNode } from 'react'
import { Plus, TriangleAlert } from 'lucide-react'
import { useCircularDeps } from '../../hooks/useDependencyGraph'
import { useUIStore } from '../../store/uiStore'
import { Button } from '../ui/button'
import { GraphSearchInput } from './GraphSearchInput'
import { GraphStaleBadge } from './GraphStaleBadge'

interface GraphControlsProps {
  query: string
  onQueryChange: (q: string) => void
  onClear: () => void
  matchCount: number
  totalCount: number
  isSearchActive: boolean
  onFitToMatches?: () => void
  placeholder?: string
  /** Sandbox-scoped pipeline picker, rendered inline in the single toolbar row. */
  scopePicker?: ReactNode
  /** Collapsible external-sources affordance folded into the toolbar. */
  externalSources?: ReactNode
  /** Problems-only lens toggle (dims clean nodes when on). */
  problemsOnly?: boolean
  onToggleProblemsOnly?: () => void
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
  scopePicker,
  externalSources,
  problemsOnly,
  onToggleProblemsOnly,
}: GraphControlsProps) {
  const { data: circular } = useCircularDeps()
  const openCreateFlowgroupDialog = useUIStore((s) => s.openCreateFlowgroupDialog)

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

      {scopePicker}

      {onToggleProblemsOnly && (
        <Button
          size="xs"
          variant={problemsOnly ? 'default' : 'outline'}
          aria-pressed={problemsOnly}
          onClick={onToggleProblemsOnly}
          title="Show only flowgroups with errors or warnings"
        >
          <TriangleAlert aria-hidden="true" />
          Problems only
        </Button>
      )}

      {externalSources}

      <div className="flex-1" />

      <GraphStaleBadge />

      <Button size="xs" variant="outline" onClick={() => openCreateFlowgroupDialog()}>
        <Plus aria-hidden="true" />
        New flowgroup
      </Button>

      {circular?.has_circular && (
        <div className="flex items-center gap-1.5 rounded-md border border-warning/25 bg-warning/10 px-2 py-1 text-2xs text-foreground">
          <TriangleAlert className="size-3.5 shrink-0 text-warning" aria-hidden="true" />
          <span>{circular.total_cycles} circular dep{circular.total_cycles > 1 ? 's' : ''}</span>
        </div>
      )}
    </div>
  )
}
