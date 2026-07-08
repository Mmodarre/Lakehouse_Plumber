import { useState, useMemo } from 'react'
import {
  ArrowDown,
  ArrowDownToLine,
  ArrowUp,
  ArrowUpDown,
  Boxes,
  Database,
  FlaskConical,
  Plus,
  Search,
  Wand2,
} from 'lucide-react'
import type { LucideIcon } from 'lucide-react'
import { useFlowgroups } from '../../hooks/useFlowgroups'
import { useUIStore } from '../../store/uiStore'
import type { FlowgroupSummary } from '../../types/api'
import { EmptyState } from '../common/EmptyState'
import { TableSkeleton } from '../common/SkeletonLoader'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select'
import { cn } from '@/lib/utils'

type SortKey = 'name' | 'pipeline' | 'action_count' | 'source_file'
type SortDir = 'asc' | 'desc'

/** Sentinel for the "no type filter" select entry (Radix Select forbids
 * an empty-string item value). Action types are load/transform/write/test,
 * so this can never collide with a real value. */
const ALL_TYPES = 'all'

/** Unified action-kind badge recipe — the same `--kind-*` tokens the DAG
 * accents use (12% tinted fill, 25% border, kind-colored text + icon), so
 * "transform" is violet and "write" is green everywhere. */
const ACTION_KIND_BADGE: Record<string, { icon: LucideIcon; className: string }> = {
  load: {
    icon: ArrowDownToLine,
    className: 'border-kind-load/25 bg-kind-load/12 text-kind-load',
  },
  transform: {
    icon: Wand2,
    className: 'border-kind-transform/25 bg-kind-transform/12 text-kind-transform',
  },
  write: {
    icon: Database,
    className: 'border-kind-write/25 bg-kind-write/12 text-kind-write',
  },
  test: {
    icon: FlaskConical,
    className: 'border-kind-test/25 bg-kind-test/12 text-kind-test',
  },
}

function ActionKindBadge({ kind }: { kind: string }) {
  const spec = ACTION_KIND_BADGE[kind]
  if (!spec) {
    return (
      <Badge variant="outline" className="rounded-sm px-1.5 text-2xs">
        {kind}
      </Badge>
    )
  }
  const Icon = spec.icon
  return (
    <Badge className={cn('h-5 rounded-sm border px-1.5 text-2xs', spec.className)}>
      <Icon className="size-2.5" aria-hidden="true" />
      {kind}
    </Badge>
  )
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

function HeaderLabel({ label }: { label: string }) {
  return (
    <span className="text-2xs font-semibold uppercase tracking-[0.05em] text-muted-foreground">
      {label}
    </span>
  )
}

export function FlowgroupTable() {
  const { data, isLoading } = useFlowgroups()
  const openFlowgroupEditor = useUIStore((s) => s.openFlowgroupEditor)
  const openCreateFlowgroupDialog = useUIStore((s) => s.openCreateFlowgroupDialog)
  const [filter, setFilter] = useState('')
  const [typeFilter, setTypeFilter] = useState<string>('')
  const [sortKey, setSortKey] = useState<SortKey>('name')
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
    if (!data?.flowgroups) return []
    let items = data.flowgroups

    if (filter) {
      const q = filter.toLowerCase()
      items = items.filter(
        (fg) =>
          fg.name.toLowerCase().includes(q) ||
          fg.pipeline.toLowerCase().includes(q),
      )
    }

    if (typeFilter) {
      items = items.filter((fg) => fg.action_types.includes(typeFilter))
    }

    items = [...items].sort((a, b) => {
      const aVal = a[sortKey as keyof FlowgroupSummary]
      const bVal = b[sortKey as keyof FlowgroupSummary]
      const cmp = String(aVal).localeCompare(String(bVal), undefined, { numeric: true })
      return sortDir === 'asc' ? cmp : -cmp
    })

    return items
  }, [data, filter, typeFilter, sortKey, sortDir])

  const allTypes = useMemo(() => {
    if (!data?.flowgroups) return []
    const types = new Set<string>()
    data.flowgroups.forEach((fg) => fg.action_types.forEach((t) => types.add(t)))
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
      {/* Toolbar */}
      <div className="flex items-center gap-3 border-b border-border px-4 py-2">
        <div className="flex items-center gap-2">
          <span className="text-sm font-semibold text-foreground">Flowgroups</span>
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
            className="h-8 w-[200px] pl-8 text-sm"
          />
        </div>

        <Select
          value={typeFilter === '' ? ALL_TYPES : typeFilter}
          onValueChange={(v) => setTypeFilter(v === ALL_TYPES ? '' : v)}
        >
          <SelectTrigger size="sm" className="text-sm" aria-label="Filter by action type">
            <SelectValue placeholder="Type" />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value={ALL_TYPES}>All types</SelectItem>
            {allTypes.map((t) => (
              <SelectItem key={t} value={t}>
                {t}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>

        <div className="ml-auto">
          <Button size="sm" onClick={openCreateFlowgroupDialog}>
            <Plus aria-hidden="true" />
            New Flowgroup
          </Button>
        </div>
      </div>

      {/* Table */}
      <div className="flex-1 overflow-auto">
        {filtered.length === 0 ? (
          <EmptyState
            icon={Boxes}
            title="No flowgroups"
            message={
              hasFilter
                ? 'No flowgroups match the current filter.'
                : 'Create a flowgroup to get started.'
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
                : { label: 'New Flowgroup', onClick: openCreateFlowgroupDialog }
            }
          />
        ) : (
          <table className="w-full text-sm">
            <thead className="sticky top-0 z-10 bg-card/80 backdrop-blur">
              <tr className="border-b border-border">
                <th aria-sort={ariaSort('name')} className="h-9 px-3 text-left">
                  <SortHeader label="Name" sortKey="name" currentSort={sortKey} currentDir={sortDir} onSort={handleSort} />
                </th>
                <th aria-sort={ariaSort('pipeline')} className="h-9 px-3 text-left">
                  <SortHeader label="Pipeline" sortKey="pipeline" currentSort={sortKey} currentDir={sortDir} onSort={handleSort} />
                </th>
                <th className="h-9 px-3 text-left">
                  <HeaderLabel label="Type" />
                </th>
                <th aria-sort={ariaSort('action_count')} className="h-9 px-3 text-right">
                  <SortHeader label="Actions" sortKey="action_count" currentSort={sortKey} currentDir={sortDir} onSort={handleSort} />
                </th>
                <th className="h-9 px-3 text-left">
                  <HeaderLabel label="Presets" />
                </th>
                <th className="h-9 px-3 text-left">
                  <HeaderLabel label="Template" />
                </th>
                <th aria-sort={ariaSort('source_file')} className="h-9 px-3 text-left">
                  <SortHeader label="Source" sortKey="source_file" currentSort={sortKey} currentDir={sortDir} onSort={handleSort} />
                </th>
              </tr>
            </thead>
            <tbody>
              {filtered.map((fg) => (
                <tr
                  key={fg.name}
                  className="h-9 cursor-pointer border-b border-border/60 transition-colors hover:bg-muted/50"
                  onClick={() => openFlowgroupEditor(fg.name, fg.pipeline)}
                >
                  <td className="px-3 font-medium text-foreground">{fg.name}</td>
                  <td className="px-3 text-muted-foreground">{fg.pipeline}</td>
                  <td className="px-3">
                    <div className="flex flex-wrap gap-1">
                      {fg.action_types.map((t) => (
                        <ActionKindBadge key={t} kind={t} />
                      ))}
                    </div>
                  </td>
                  <td className="px-3 text-right tabular-nums text-muted-foreground">
                    {fg.action_count}
                  </td>
                  <td className="px-3 text-muted-foreground">
                    {fg.presets.length > 0 ? (
                      fg.presets.join(', ')
                    ) : (
                      <span className="text-muted-foreground/60">&mdash;</span>
                    )}
                  </td>
                  <td className="px-3 text-muted-foreground">
                    {fg.template ?? <span className="text-muted-foreground/60">&mdash;</span>}
                  </td>
                  <td
                    className="max-w-[200px] truncate px-3 font-mono text-xs text-muted-foreground"
                    title={fg.source_file}
                  >
                    {fg.source_file.split('/').pop()}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>
    </div>
  )
}
