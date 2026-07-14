import { useMemo, useState } from 'react'
import { ArrowRightToLine, Search, Table2 } from 'lucide-react'
import { useTables } from '../../../hooks/useTables'
import { useMapEnrichment, type MapSeverity } from '../../../hooks/useMapEnrichment'
import { useUIStore } from '../../../store/uiStore'
import { useWorkspaceStore } from '../../../store/workspaceStore'
import type { TableSummary } from '../../../types/api'
import { SectionHead, SeverityDot } from './explorerPrimitives'
import { groupTables, isSinkFqn, tableLeafName } from './explorerData'
import { cn } from '@/lib/utils'

// ── TablesLens — the Tables explorer lens (T1.3) ─────────────
//
// Recomposes the DATA path of components/table/TablesTable into a compact
// navigator: search box + schema-grouped rows (TABLE vs SINK glyph, producing
// flowgroup hint), each opening a Table-detail center tab. TablesTable itself
// is untouched (it retires later).

function TableRow({
  table,
  active,
  severity,
  onOpen,
}: {
  table: TableSummary
  active: boolean
  severity: MapSeverity | undefined
  onOpen: (fqn: string) => void
}) {
  const sink = isSinkFqn(table.full_name)
  const Glyph = sink ? ArrowRightToLine : Table2
  return (
    <button
      type="button"
      onClick={() => onOpen(table.full_name)}
      aria-current={active ? 'true' : undefined}
      title={table.full_name}
      className={cn(
        'relative flex w-full items-center gap-2 py-1 pr-3 pl-3 text-left transition-colors',
        active ? 'bg-accent-weak font-medium' : 'hover:bg-card',
      )}
    >
      {active && (
        <span
          aria-hidden="true"
          className="absolute inset-y-1 left-0 w-0.5 rounded-full bg-primary"
        />
      )}
      <Glyph
        className={cn('size-3.5 shrink-0', sink ? 'text-warning' : 'text-kind-write')}
        aria-hidden="true"
      />
      <span className="min-w-0 flex-1 truncate font-mono text-xs text-foreground">
        {tableLeafName(table.full_name)}
      </span>
      <SeverityDot severity={severity} />
      <span className="max-w-[40%] shrink-0 truncate font-mono text-2xs text-faint">
        {table.flowgroup}
      </span>
    </button>
  )
}

export function TablesLens() {
  const selectedEnv = useUIStore((s) => s.selectedEnv)
  const openTableDetail = useWorkspaceStore((s) => s.openTableDetail)
  const activePath = useWorkspaceStore((s) => s.activePath)
  const { data, isLoading } = useTables(selectedEnv)
  const enrichment = useMapEnrichment(selectedEnv)
  const [query, setQuery] = useState('')

  const groups = useMemo(() => groupTables(data?.tables ?? [], query), [data, query])

  return (
    <div className="flex h-full min-h-0 flex-col pb-4">
      {/* Search */}
      <div className="px-1.5 pt-2 pb-1">
        <div className="flex items-center gap-2 rounded-sm border border-border bg-card px-2.5 py-1.5 focus-within:border-accent-line">
          <Search className="size-3.5 shrink-0 text-faint" aria-hidden="true" />
          <input
            type="text"
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            placeholder="Filter tables & views…"
            aria-label="Filter tables"
            className="min-w-0 flex-1 bg-transparent text-xs text-foreground outline-none placeholder:text-faint"
          />
        </div>
      </div>

      <div className="min-h-0 flex-1 overflow-auto">
        {!selectedEnv ? (
          <p className="px-3 py-2 text-2xs text-faint">Select an environment to list tables.</p>
        ) : isLoading ? (
          <p className="px-3 py-2 text-2xs text-faint">Loading…</p>
        ) : groups.length === 0 ? (
          <p className="px-3 py-2 text-2xs text-faint">
            {query ? 'No tables match the filter.' : 'No tables resolved for this environment.'}
          </p>
        ) : (
          groups.map((group) => (
            <div key={group.label}>
              <SectionHead>{group.label}</SectionHead>
              {group.tables.map((table, idx) => {
                const id = `table:${table.full_name}`
                return (
                  <TableRow
                    key={`${table.full_name}-${idx}`}
                    table={table}
                    active={activePath === id}
                    severity={enrichment.severityFor(table.pipeline, table.flowgroup)}
                    onOpen={openTableDetail}
                  />
                )
              })}
            </div>
          ))
        )}
      </div>
    </div>
  )
}
