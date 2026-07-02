import { useState, useMemo } from 'react'
import { useTables } from '../../hooks/useTables'
import { useUIStore } from '../../store/uiStore'
import type { TableSummary } from '../../types/api'
import { LoadingSpinner } from '../common/LoadingSpinner'

type SortKey = 'full_name' | 'target_type' | 'pipeline' | 'flowgroup' | 'write_mode' | 'source_file'
type SortDir = 'asc' | 'desc'

const TARGET_TYPE_BADGE: Record<string, string> = {
  streaming_table: 'bg-blue-100 text-blue-700',
  materialized_view: 'bg-emerald-100 text-emerald-700',
  sink: 'bg-orange-100 text-orange-700',
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
      className="flex items-center gap-1 text-left text-[11px] font-semibold uppercase tracking-wider text-slate-500 hover:text-slate-700"
      onClick={() => onSort(sortKey)}
    >
      {label}
      {isActive && (
        <span className="text-[9px]">{currentDir === 'asc' ? '\u25B2' : '\u25BC'}</span>
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

  if (isLoading) return <LoadingSpinner className="py-4" />

  return (
    <div className="flex h-full flex-col bg-white">
      {/* Warnings banner */}
      {data?.warnings && data.warnings.length > 0 && (
        <div className="border-b border-amber-200 bg-amber-50 px-4 py-2 text-xs text-amber-800">
          <span className="font-semibold">Warnings:</span>{' '}
          {data.warnings.length} flowgroup(s) could not be resolved.
          <details className="mt-1">
            <summary className="cursor-pointer text-amber-600 hover:text-amber-800">
              Show details
            </summary>
            <ul className="mt-1 list-inside list-disc">
              {data.warnings.map((w, i) => (
                <li key={i}>{w}</li>
              ))}
            </ul>
          </details>
        </div>
      )}

      {/* Toolbar */}
      <div className="flex items-center gap-3 border-b border-slate-200 px-4 py-2">
        <span className="text-xs font-semibold text-slate-700">
          Tables <span className="font-normal text-slate-400">{filtered.length}</span>
        </span>

        <div className="relative ml-4">
          <svg className="absolute left-2 top-1/2 h-3.5 w-3.5 -translate-y-1/2 text-slate-400" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z" />
          </svg>
          <input
            type="text"
            placeholder="Filter by name..."
            value={filter}
            onChange={(e) => setFilter(e.target.value)}
            className="rounded border border-slate-200 py-1 pl-7 pr-2 text-xs text-slate-700 placeholder-slate-400 focus:border-blue-400 focus:outline-none"
            style={{ width: 220 }}
          />
        </div>

        <select
          value={typeFilter}
          onChange={(e) => setTypeFilter(e.target.value)}
          className="rounded border border-slate-200 px-2 py-1 text-xs text-slate-700 focus:border-blue-400 focus:outline-none"
        >
          <option value="">Target Type</option>
          {allTypes.map((t) => (
            <option key={t} value={t}>{t.replace('_', ' ')}</option>
          ))}
        </select>
      </div>

      {/* Table */}
      <div className="flex-1 overflow-auto">
        <table className="w-full text-xs">
          <thead className="sticky top-0 bg-slate-50">
            <tr className="border-b border-slate-200">
              <th className="px-4 py-2 text-left">
                <SortHeader label="Table" sortKey="full_name" currentSort={sortKey} currentDir={sortDir} onSort={handleSort} />
              </th>
              <th className="px-4 py-2 text-left">
                <SortHeader label="Target Type" sortKey="target_type" currentSort={sortKey} currentDir={sortDir} onSort={handleSort} />
              </th>
              <th className="px-4 py-2 text-left">
                <SortHeader label="Pipeline" sortKey="pipeline" currentSort={sortKey} currentDir={sortDir} onSort={handleSort} />
              </th>
              <th className="px-4 py-2 text-left">
                <SortHeader label="Flowgroup" sortKey="flowgroup" currentSort={sortKey} currentDir={sortDir} onSort={handleSort} />
              </th>
              <th className="px-4 py-2 text-left">
                <SortHeader label="Write Mode" sortKey="write_mode" currentSort={sortKey} currentDir={sortDir} onSort={handleSort} />
              </th>
              <th className="px-4 py-2 text-left">
                <span className="text-[11px] font-semibold uppercase tracking-wider text-slate-500">SCD Type</span>
              </th>
              <th className="px-4 py-2 text-left">
                <SortHeader label="Source" sortKey="source_file" currentSort={sortKey} currentDir={sortDir} onSort={handleSort} />
              </th>
            </tr>
          </thead>
          <tbody>
            {filtered.map((t, idx) => (
              <tr
                key={`${t.full_name}-${t.flowgroup}-${idx}`}
                className="cursor-pointer border-b border-slate-100 transition-colors hover:bg-blue-50/50"
                onClick={() => openFlowgroupEditor(t.flowgroup, t.pipeline)}
              >
                <td className="px-4 py-2">
                  <span className="font-medium text-slate-800">{t.full_name}</span>
                </td>
                <td className="px-4 py-2">
                  <span
                    className={`rounded px-1.5 py-0.5 text-[10px] font-medium ${TARGET_TYPE_BADGE[t.target_type] ?? 'bg-slate-100 text-slate-600'}`}
                  >
                    {t.target_type.replace('_', ' ')}
                  </span>
                </td>
                <td className="px-4 py-2 text-slate-600">{t.pipeline}</td>
                <td className="px-4 py-2">
                  <span className="font-medium text-blue-700 hover:underline">{t.flowgroup}</span>
                </td>
                <td className="px-4 py-2 text-slate-600">{t.write_mode ?? '-'}</td>
                <td className="px-4 py-2 text-slate-600">{t.scd_type ?? '-'}</td>
                <td className="px-4 py-2 text-slate-400 truncate max-w-[200px]" title={t.source_file}>
                  {t.source_file.split('/').pop()}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}
