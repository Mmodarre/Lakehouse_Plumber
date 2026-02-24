import { useState, useMemo } from 'react'
import { useFlowgroups } from '../../hooks/useFlowgroups'
import { useUIStore } from '../../store/uiStore'
import type { FlowgroupSummary } from '../../types/api'
import { LoadingSpinner } from '../common/LoadingSpinner'

type SortKey = 'name' | 'pipeline' | 'action_count' | 'source_file'
type SortDir = 'asc' | 'desc'

const ACTION_TYPE_BADGE: Record<string, string> = {
  load: 'bg-blue-100 text-blue-700',
  transform: 'bg-emerald-100 text-emerald-700',
  write: 'bg-orange-100 text-orange-700',
  test: 'bg-purple-100 text-purple-700',
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

export function FlowgroupTable() {
  const { data, isLoading } = useFlowgroups()
  const openFlowgroupEditor = useUIStore((s) => s.openFlowgroupEditor)
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

  if (isLoading) return <LoadingSpinner className="py-4" />

  return (
    <div className="flex h-full flex-col bg-white">
      {/* Toolbar */}
      <div className="flex items-center gap-3 border-b border-slate-200 px-4 py-2">
        <span className="text-xs font-semibold text-slate-700">
          Flowgroups <span className="font-normal text-slate-400">{filtered.length}</span>
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
            style={{ width: 180 }}
          />
        </div>

        <select
          value={typeFilter}
          onChange={(e) => setTypeFilter(e.target.value)}
          className="rounded border border-slate-200 px-2 py-1 text-xs text-slate-700 focus:border-blue-400 focus:outline-none"
        >
          <option value="">Type</option>
          {allTypes.map((t) => (
            <option key={t} value={t}>{t}</option>
          ))}
        </select>
      </div>

      {/* Table */}
      <div className="flex-1 overflow-auto">
        <table className="w-full text-xs">
          <thead className="sticky top-0 bg-slate-50">
            <tr className="border-b border-slate-200">
              <th className="px-4 py-2 text-left">
                <SortHeader label="Name" sortKey="name" currentSort={sortKey} currentDir={sortDir} onSort={handleSort} />
              </th>
              <th className="px-4 py-2 text-left">
                <SortHeader label="Pipeline" sortKey="pipeline" currentSort={sortKey} currentDir={sortDir} onSort={handleSort} />
              </th>
              <th className="px-4 py-2 text-left">
                <span className="text-[11px] font-semibold uppercase tracking-wider text-slate-500">Type</span>
              </th>
              <th className="px-4 py-2 text-left">
                <SortHeader label="Actions" sortKey="action_count" currentSort={sortKey} currentDir={sortDir} onSort={handleSort} />
              </th>
              <th className="px-4 py-2 text-left">
                <span className="text-[11px] font-semibold uppercase tracking-wider text-slate-500">Presets</span>
              </th>
              <th className="px-4 py-2 text-left">
                <span className="text-[11px] font-semibold uppercase tracking-wider text-slate-500">Template</span>
              </th>
              <th className="px-4 py-2 text-left">
                <SortHeader label="Source" sortKey="source_file" currentSort={sortKey} currentDir={sortDir} onSort={handleSort} />
              </th>
            </tr>
          </thead>
          <tbody>
            {filtered.map((fg) => (
              <tr
                key={fg.name}
                className="cursor-pointer border-b border-slate-100 transition-colors hover:bg-blue-50/50"
                onClick={() => openFlowgroupEditor(fg.name, fg.pipeline)}
              >
                <td className="px-4 py-2">
                  <span className="font-medium text-blue-700 hover:underline">{fg.name}</span>
                </td>
                <td className="px-4 py-2 text-slate-600">{fg.pipeline}</td>
                <td className="px-4 py-2">
                  <div className="flex flex-wrap gap-1">
                    {fg.action_types.map((t) => (
                      <span
                        key={t}
                        className={`rounded px-1.5 py-0.5 text-[10px] font-medium ${ACTION_TYPE_BADGE[t] ?? 'bg-slate-100 text-slate-600'}`}
                      >
                        {t}
                      </span>
                    ))}
                  </div>
                </td>
                <td className="px-4 py-2 text-slate-600">{fg.action_count}</td>
                <td className="px-4 py-2 text-slate-500">
                  {fg.presets.length > 0 ? fg.presets.join(', ') : '-'}
                </td>
                <td className="px-4 py-2 text-slate-500">
                  {fg.template ?? '-'}
                </td>
                <td className="px-4 py-2 text-slate-400 truncate max-w-[200px]" title={fg.source_file}>
                  {fg.source_file.split('/').pop()}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}
