import { useState, useMemo } from 'react'
import {
  ArrowDown,
  ArrowRightToLine,
  ArrowUp,
  ArrowUpDown,
  Search,
  Table2,
  TriangleAlert,
  Zap,
} from 'lucide-react'
import type { LucideIcon } from 'lucide-react'
import { useTables } from '../../hooks/useTables'
import { useUIStore } from '../../store/uiStore'
import type { TableSummary } from '../../types/api'
import { EmptyState } from '../common/EmptyState'
import { TableSkeleton } from '../common/SkeletonLoader'
import { Badge } from '@/components/ui/badge'
import { Input } from '@/components/ui/input'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select'
import {
  Tooltip,
  TooltipContent,
  TooltipTrigger,
} from '@/components/ui/tooltip'
import { cn } from '@/lib/utils'

type SortKey = 'full_name' | 'target_type' | 'pipeline' | 'flowgroup' | 'write_mode' | 'source_file'
type SortDir = 'asc' | 'desc'

/** Sentinel for the "no type filter" select entry (Radix Select forbids
 * an empty-string item value). Target types are snake_case identifiers,
 * so this can never collide with a real value. */
const ALL_TYPES = 'all'

/** Short, non-wrapping target-type badges (tinted recipe: 12% fill, 25%
 * border, semantic-token text + icon). Full name lives in the tooltip. */
const TARGET_TYPE_BADGE: Record<
  string,
  { label: string; icon: LucideIcon; className: string; description: string }
> = {
  streaming_table: {
    label: 'ST',
    icon: Zap,
    className: 'border-info/25 bg-info/12 text-info',
    description: 'Streaming table',
  },
  materialized_view: {
    label: 'MV',
    icon: Table2,
    className: 'border-success/25 bg-success/12 text-success',
    description: 'Materialized view',
  },
  sink: {
    label: 'Sink',
    icon: ArrowRightToLine,
    className: 'border-warning/25 bg-warning/12 text-warning',
    description: 'Sink',
  },
}

function TargetTypeBadge({ targetType }: { targetType: string }) {
  const spec = TARGET_TYPE_BADGE[targetType]
  if (!spec) {
    return (
      <Badge variant="outline" className="rounded-sm px-1.5 text-2xs">
        {targetType.replace('_', ' ')}
      </Badge>
    )
  }
  const Icon = spec.icon
  return (
    <Tooltip>
      <TooltipTrigger asChild>
        <Badge className={cn('h-5 rounded-sm border px-1.5 text-2xs', spec.className)}>
          <Icon className="size-2.5" aria-hidden="true" />
          {spec.label}
        </Badge>
      </TooltipTrigger>
      <TooltipContent>{spec.description}</TooltipContent>
    </Tooltip>
  )
}

/** Fully-qualified table name in mono, with the `catalog.schema.` prefix
 * de-emphasized so the table segment carries the weight. */
function FullTableName({ name }: { name: string }) {
  const lastDot = name.lastIndexOf('.')
  if (lastDot === -1) {
    return <span className="font-mono text-xs text-foreground">{name}</span>
  }
  return (
    <span className="font-mono text-xs">
      <span className="text-muted-foreground">{name.slice(0, lastDot + 1)}</span>
      <span className="font-medium text-foreground">{name.slice(lastDot + 1)}</span>
    </span>
  )
}

/** Merged write-mode + SCD label; null for the default (`standard`, no SCD)
 * case so 70-odd rows of `standard` noise collapse to an em dash. */
function modeLabel(t: TableSummary): string | null {
  const parts: string[] = []
  if (t.write_mode != null && t.write_mode !== 'standard') parts.push(t.write_mode)
  if (t.scd_type != null) parts.push(`SCD ${t.scd_type}`)
  return parts.length > 0 ? parts.join(' · ') : null
}

function SortHeader({
  label,
  sortKey,
  currentSort,
  currentDir,
  onSort,
}: {
  label: string
  sortKey: SortKey
  currentSort: SortKey
  currentDir: SortDir
  onSort: (key: SortKey) => void
}) {
  const isActive = currentSort === sortKey
  return (
    <button
      type="button"
      aria-label={`Sort by ${label}`}
      className={cn(
        'group inline-flex items-center gap-1 text-left text-2xs font-semibold uppercase tracking-[0.05em] transition-colors',
        isActive
          ? 'text-foreground'
          : 'text-muted-foreground hover:text-foreground',
      )}
      onClick={() => onSort(sortKey)}
    >
      {label}
      {isActive ? (
        currentDir === 'asc' ? (
          <ArrowUp className="size-3" aria-hidden="true" />
        ) : (
          <ArrowDown className="size-3" aria-hidden="true" />
        )
      ) : (
        <ArrowUpDown
          className="size-3 opacity-0 transition-opacity group-hover:opacity-100"
          aria-hidden="true"
        />
      )}
    </button>
  )
}

export function TablesTable() {
  const selectedEnv = useUIStore((s) => s.selectedEnv)
  const pipelineFilter = useUIStore((s) => s.pipelineFilter)
  const openFlowgroupEditor = useUIStore((s) => s.openFlowgroupEditor)
  const { data, isLoading } = useTables(selectedEnv, pipelineFilter ?? undefined)

  const [filter, setFilter] = useState('')
  const [typeFilter, setTypeFilter] = useState<string>('')
  const [sortKey, setSortKey] = useState<SortKey>('full_name')
  const [sortDir, setSortDir] = useState<SortDir>('asc')

  const handleSort = (key: SortKey) => {
    if (sortKey === key) {
      setSortDir(sortDir === 'asc' ? 'desc' : 'asc')
    } else {
      setSortKey(key)
      setSortDir('asc')
    }
  }

  /** `aria-sort` value for a sortable column header. */
  const ariaSort = (key: SortKey): 'ascending' | 'descending' | 'none' =>
    sortKey === key ? (sortDir === 'asc' ? 'ascending' : 'descending') : 'none'

  const filtered = useMemo(() => {
    if (!data?.tables) return []
    let items = data.tables

    if (filter) {
      const q = filter.toLowerCase()
      items = items.filter(
        (t) =>
          t.full_name.toLowerCase().includes(q) ||
          t.flowgroup.toLowerCase().includes(q) ||
          t.pipeline.toLowerCase().includes(q),
      )
    }

    if (typeFilter) {
      items = items.filter((t) => t.target_type === typeFilter)
    }

    items = [...items].sort((a, b) => {
      const aVal = a[sortKey as keyof TableSummary]
      const bVal = b[sortKey as keyof TableSummary]
      const cmp = String(aVal ?? '').localeCompare(String(bVal ?? ''), undefined, { numeric: true })
      return sortDir === 'asc' ? cmp : -cmp
    })

    return items
  }, [data, filter, typeFilter, sortKey, sortDir])

  const allTypes = useMemo(() => {
    if (!data?.tables) return []
    const types = new Set<string>()
    data.tables.forEach((t) => types.add(t.target_type))
    return [...types].sort()
  }, [data])

  if (isLoading) {
    return (
      <div className="flex h-full flex-col bg-card">
        <TableSkeleton rows={8} />
      </div>
    )
  }

  const hasFilter = filter !== '' || typeFilter !== ''

  return (
    <div className="flex h-full flex-col bg-card">
        {/* Warnings banner */}
        {data?.warnings && data.warnings.length > 0 && (
          <div className="flex items-start gap-2 border-b border-warning/25 bg-warning/12 px-4 py-2 text-xs text-foreground">
            <TriangleAlert
              className="mt-0.5 size-3.5 shrink-0 text-warning"
              aria-hidden="true"
            />
            <div className="min-w-0 flex-1">
              <span className="font-semibold">Warnings:</span>{' '}
              {data.warnings.length} flowgroup(s) could not be resolved.
              <details className="mt-1">
                <summary className="cursor-pointer text-muted-foreground hover:text-foreground">
                  Show details
                </summary>
                <ul className="mt-1 list-inside list-disc text-muted-foreground">
                  {data.warnings.map((w, i) => (
                    <li key={i}>{w}</li>
                  ))}
                </ul>
              </details>
            </div>
          </div>
        )}

        {/* Toolbar */}
        <div className="flex items-center gap-3 border-b border-border px-4 py-2">
          <div className="flex items-center gap-2">
            <span className="text-sm font-semibold text-foreground">Tables</span>
            <Badge variant="secondary" className="tabular-nums">
              {filtered.length}
            </Badge>
          </div>

          <div className="relative ml-2">
            <Search
              className="absolute inset-y-0 left-2.5 my-auto size-3.5 text-muted-foreground"
              aria-hidden="true"
            />
            <Input
              type="text"
              placeholder="Filter by name…"
              value={filter}
              onChange={(e) => setFilter(e.target.value)}
              className="h-8 w-[220px] pl-8 text-sm"
            />
          </div>

          <Select
            value={typeFilter === '' ? ALL_TYPES : typeFilter}
            onValueChange={(v) => setTypeFilter(v === ALL_TYPES ? '' : v)}
          >
            <SelectTrigger size="sm" className="text-sm" aria-label="Filter by target type">
              <SelectValue placeholder="Target type" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value={ALL_TYPES}>All target types</SelectItem>
              {allTypes.map((t) => (
                <SelectItem key={t} value={t}>
                  {t.replace('_', ' ')}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>

        {/* Table */}
        <div className="flex-1 overflow-auto">
          {filtered.length === 0 ? (
            <EmptyState
              icon={Table2}
              title="No tables"
              message={
                hasFilter
                  ? 'No tables match the current filter.'
                  : 'No write targets resolved for this environment.'
              }
              action={
                hasFilter
                  ? {
                      label: 'Clear filter',
                      onClick: () => {
                        setFilter('')
                        setTypeFilter('')
                      },
                      variant: 'outline',
                    }
                  : undefined
              }
            />
          ) : (
            <table className="w-full text-sm">
              <thead className="sticky top-0 z-10 bg-card/80 backdrop-blur">
                <tr className="border-b border-border">
                  <th aria-sort={ariaSort('full_name')} className="h-9 px-3 text-left">
                    <SortHeader label="Table" sortKey="full_name" currentSort={sortKey} currentDir={sortDir} onSort={handleSort} />
                  </th>
                  <th aria-sort={ariaSort('target_type')} className="h-9 px-3 text-left">
                    <SortHeader label="Type" sortKey="target_type" currentSort={sortKey} currentDir={sortDir} onSort={handleSort} />
                  </th>
                  <th aria-sort={ariaSort('pipeline')} className="h-9 px-3 text-left">
                    <SortHeader label="Pipeline" sortKey="pipeline" currentSort={sortKey} currentDir={sortDir} onSort={handleSort} />
                  </th>
                  <th aria-sort={ariaSort('flowgroup')} className="h-9 px-3 text-left">
                    <SortHeader label="Flowgroup" sortKey="flowgroup" currentSort={sortKey} currentDir={sortDir} onSort={handleSort} />
                  </th>
                  <th aria-sort={ariaSort('write_mode')} className="h-9 px-3 text-left">
                    <SortHeader label="Mode" sortKey="write_mode" currentSort={sortKey} currentDir={sortDir} onSort={handleSort} />
                  </th>
                  <th aria-sort={ariaSort('source_file')} className="h-9 px-3 text-left">
                    <SortHeader label="Source" sortKey="source_file" currentSort={sortKey} currentDir={sortDir} onSort={handleSort} />
                  </th>
                </tr>
              </thead>
              <tbody>
                {filtered.map((t, idx) => {
                  const mode = modeLabel(t)
                  return (
                    <tr
                      key={`${t.full_name}-${t.flowgroup}-${idx}`}
                      className="h-9 cursor-pointer border-b border-border/60 transition-colors hover:bg-muted/50"
                      onClick={() => openFlowgroupEditor(t.flowgroup, t.pipeline)}
                    >
                      <td className="px-3">
                        <FullTableName name={t.full_name} />
                      </td>
                      <td className="px-3">
                        <TargetTypeBadge targetType={t.target_type} />
                      </td>
                      <td className="px-3 text-muted-foreground">{t.pipeline}</td>
                      <td className="px-3 font-medium text-foreground">{t.flowgroup}</td>
                      <td className="px-3">
                        {mode ? (
                          <Badge
                            variant="outline"
                            className="h-5 rounded-sm px-1.5 font-mono text-2xs text-muted-foreground"
                          >
                            {mode}
                          </Badge>
                        ) : (
                          <span className="text-muted-foreground/60">&mdash;</span>
                        )}
                      </td>
                      <td
                        className="max-w-[200px] truncate px-3 font-mono text-xs text-muted-foreground"
                        title={t.source_file}
                      >
                        {t.source_file.split('/').pop()}
                      </td>
                    </tr>
                  )
                })}
              </tbody>
            </table>
          )}
        </div>
      </div>
  )
}
