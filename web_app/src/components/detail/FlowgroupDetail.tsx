import { useFlowgroupDetail, useFlowgroupResolved } from '../../hooks/useFlowgroups'
import { useUIStore } from '../../store/uiStore'
import { LoadingSpinner } from '../common/LoadingSpinner'
import { JsonTree } from './JsonTree'

const ACTION_TYPE_COLORS: Record<string, string> = {
  load: 'bg-blue-100 text-blue-700',
  transform: 'bg-green-100 text-green-700',
  write: 'bg-orange-100 text-orange-700',
  test: 'bg-purple-100 text-purple-700',
}

export function FlowgroupDetail({ name }: { name: string }) {
  const { selectedEnv, openModal } = useUIStore()
  const { data: detail, isLoading: loadingDetail } = useFlowgroupDetail(name)
  const { data: resolved, isLoading: loadingResolved } = useFlowgroupResolved(name, selectedEnv)

  if (loadingDetail) return <LoadingSpinner className="py-8" />

  const fg = detail?.flowgroup as Record<string, unknown> | undefined
  const actions = (fg?.actions ?? []) as Array<Record<string, unknown>>

  return (
    <div className="space-y-4">
      {/* Source file */}
      {detail?.source_file && (
        <p className="text-[10px] text-slate-400 break-all">{detail.source_file}</p>
      )}

      {/* Actions table */}
      <div>
        <h3 className="mb-1.5 text-[11px] font-semibold uppercase tracking-wider text-slate-400">
          Actions ({actions.length})
        </h3>
        <div className="space-y-1">
          {actions.map((action, i) => {
            const actionType = (action.type as string) ?? 'unknown'
            const actionName = (action.name as string) ?? `action-${i}`
            const target = (action.target as string) ?? ''
            return (
              <div key={i} className="flex items-center gap-2 rounded bg-slate-50 px-2 py-1.5">
                <span className={`rounded px-1.5 py-0.5 text-[10px] font-medium ${ACTION_TYPE_COLORS[actionType] ?? 'bg-slate-100 text-slate-600'}`}>
                  {actionType}
                </span>
                <span className="truncate text-xs text-slate-700">{actionName}</span>
                {target && (
                  <span className="ml-auto truncate text-[10px] text-slate-400">{target}</span>
                )}
              </div>
            )
          })}
        </div>
      </div>

      {/* Applied presets */}
      {resolved?.applied_presets && resolved.applied_presets.length > 0 && (
        <div>
          <h3 className="mb-1.5 text-[11px] font-semibold uppercase tracking-wider text-slate-400">
            Applied Presets
          </h3>
          <div className="flex flex-wrap gap-1">
            {resolved.applied_presets.map((preset) => (
              <button
                key={preset}
                className="rounded bg-violet-50 px-2 py-0.5 text-[11px] text-violet-700 hover:bg-violet-100"
                onClick={() => openModal({ type: 'preset', name: preset })}
              >
                {preset}
              </button>
            ))}
          </div>
        </div>
      )}

      {/* Applied template */}
      {resolved?.applied_template && (
        <div>
          <h3 className="mb-1.5 text-[11px] font-semibold uppercase tracking-wider text-slate-400">
            Template
          </h3>
          <button
            className="rounded bg-cyan-50 px-2 py-0.5 text-[11px] text-cyan-700 hover:bg-cyan-100"
            onClick={() => openModal({ type: 'template', name: resolved.applied_template! })}
          >
            {resolved.applied_template}
          </button>
        </div>
      )}

      {/* Resolved config */}
      <div>
        <h3 className="mb-1.5 text-[11px] font-semibold uppercase tracking-wider text-slate-400">
          Resolved Config
          {loadingResolved && <LoadingSpinner className="ml-2 inline-block" />}
        </h3>
        {resolved?.flowgroup && (
          <JsonTree data={resolved.flowgroup} />
        )}
      </div>
    </div>
  )
}
