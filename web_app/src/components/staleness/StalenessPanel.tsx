import { useStaleness } from '../../hooks/useStaleness'
import { useUIStore } from '../../store/uiStore'
import { LoadingSpinner } from '../common/LoadingSpinner'

function summarizePipelineInfo(info: unknown): string {
  if (typeof info !== 'object' || info === null) return String(info)
  const obj = info as Record<string, unknown>

  // Simple reason string (e.g. force, global_changes)
  if (typeof obj.reason === 'string') return obj.reason

  // generation_info shape: { new: [...], stale: [...], up_to_date: [...], context_stale?: [...] }
  const newCount = Array.isArray(obj.new) ? obj.new.length : 0
  const staleCount = Array.isArray(obj.stale) ? obj.stale.length : 0
  const ctxCount = Array.isArray(obj.context_stale) ? obj.context_stale.length : 0
  const parts: string[] = []
  if (newCount > 0) parts.push(`${newCount} new`)
  if (staleCount > 0) parts.push(`${staleCount} stale`)
  if (ctxCount > 0) parts.push(`${ctxCount} context`)
  return parts.length > 0 ? parts.join(' · ') : 'needs generation'
}

export function StalenessPanel() {
  const selectedEnv = useUIStore((s) => s.selectedEnv)
  const { data, isLoading, error } = useStaleness(selectedEnv)

  if (isLoading) return <LoadingSpinner className="py-8" />

  if (error) {
    return (
      <div className="rounded border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700">
        {error.message}
      </div>
    )
  }

  if (!data) return null

  return (
    <div className="space-y-4">
      {/* Work-to-do banner */}
      <div
        className={`rounded-lg px-4 py-3 text-sm font-medium ${
          data.has_work_to_do
            ? 'bg-amber-50 text-amber-800 border border-amber-200'
            : 'bg-green-50 text-green-800 border border-green-200'
        }`}
      >
        {data.has_work_to_do
          ? `${data.total_new + data.total_stale} pipelines need regeneration`
          : 'All pipelines are up to date'}
      </div>

      {/* Summary cards */}
      <div className="grid grid-cols-3 gap-3">
        <div className="rounded-lg border border-blue-200 bg-blue-50 p-3 text-center">
          <p className="text-2xl font-bold text-blue-700">{data.total_new}</p>
          <p className="text-[10px] font-medium uppercase text-blue-500">New</p>
        </div>
        <div className="rounded-lg border border-amber-200 bg-amber-50 p-3 text-center">
          <p className="text-2xl font-bold text-amber-700">{data.total_stale}</p>
          <p className="text-[10px] font-medium uppercase text-amber-500">Stale</p>
        </div>
        <div className="rounded-lg border border-green-200 bg-green-50 p-3 text-center">
          <p className="text-2xl font-bold text-green-700">{data.total_up_to_date}</p>
          <p className="text-[10px] font-medium uppercase text-green-500">Up to date</p>
        </div>
      </div>

      {/* Global changes */}
      {data.global_changes.length > 0 && (
        <div>
          <h3 className="mb-1.5 text-xs font-semibold text-slate-600">Global Changes</h3>
          <div className="space-y-1">
            {data.global_changes.map((change, i) => (
              <div key={i} className="rounded bg-amber-50 px-3 py-1.5 text-xs text-amber-700">
                {change}
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Pipeline breakdown */}
      {Object.keys(data.pipelines_needing_generation).length > 0 && (
        <div>
          <h3 className="mb-1.5 text-xs font-semibold text-slate-600">Needing Generation</h3>
          <div className="space-y-1">
            {Object.entries(data.pipelines_needing_generation).map(([name, info]) => (
              <div key={name} className="flex items-center justify-between rounded bg-slate-50 px-3 py-2">
                <span className="text-xs font-medium text-slate-700">{name}</span>
                <span className="text-[10px] text-amber-600">
                  {summarizePipelineInfo(info)}
                </span>
              </div>
            ))}
          </div>
        </div>
      )}

      {Object.keys(data.pipelines_up_to_date).length > 0 && (
        <div>
          <h3 className="mb-1.5 text-xs font-semibold text-slate-600">Up to Date</h3>
          <div className="space-y-1">
            {Object.entries(data.pipelines_up_to_date).map(([name, count]) => (
              <div key={name} className="flex items-center justify-between rounded bg-slate-50 px-3 py-2">
                <span className="text-xs text-slate-700">{name}</span>
                <span className="flex items-center gap-1 text-[10px] text-green-600">
                  <svg className="h-3 w-3" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M5 13l4 4L19 7" />
                  </svg>
                  {count} files
                </span>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  )
}
