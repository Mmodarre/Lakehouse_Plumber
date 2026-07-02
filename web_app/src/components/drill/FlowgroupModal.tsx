import { ReactFlowProvider } from '@xyflow/react'
import { useUIStore } from '../../store/uiStore'
import { useFlowgroupDetail } from '../../hooks/useFlowgroups'
import { ActionMiniGraph } from './ActionMiniGraph'

export function FlowgroupModal() {
  const { drillFlowgroup, closeFlowgroupModal, openModal, openFlowgroupEditor } = useUIStore()
  const { data: detail } = useFlowgroupDetail(drillFlowgroup?.name ?? null)

  // Escape key is handled by PipelineModal (cascading close: flowgroup first, then pipeline)

  if (!drillFlowgroup) return null

  const sourceFile = detail?.source_file

  return (
    <div
      className="fixed inset-0 flex items-center justify-center bg-black/25"
      style={{ zIndex: 45 }}
      onClick={(e) => {
        if (e.target === e.currentTarget) closeFlowgroupModal()
      }}
    >
      <div className="flex h-[70vh] w-full max-w-4xl flex-col overflow-hidden rounded-lg border border-slate-200 bg-white shadow-xl">
        {/* Header */}
        <div className="flex items-center justify-between border-b border-slate-200 px-5 py-3">
          <div>
            <span className="text-[10px] font-semibold uppercase tracking-wider text-slate-400">
              Flowgroup
            </span>
            <h2 className="text-base font-semibold text-slate-800">{drillFlowgroup.name}</h2>
          </div>
          <div className="flex items-center gap-2">
            <button
              className="rounded bg-slate-100 px-3 py-1.5 text-[11px] font-medium text-slate-700 hover:bg-slate-200 disabled:opacity-40"
              disabled={!sourceFile}
              onClick={() => openFlowgroupEditor(drillFlowgroup.name, drillFlowgroup.pipeline)}
            >
              Edit Flowgroup
            </button>
            <button
              className="rounded bg-slate-100 px-3 py-1.5 text-[11px] font-medium text-slate-700 hover:bg-slate-200"
              onClick={() => openModal({ type: 'flowgroup', name: drillFlowgroup.name })}
            >
              View Config
            </button>
            <button
              onClick={closeFlowgroupModal}
              className="rounded p-1.5 text-slate-400 hover:bg-slate-100 hover:text-slate-600"
            >
              <svg className="h-5 w-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
              </svg>
            </button>
          </div>
        </div>

        {/* Body: action-level graph */}
        <div className="flex-1 min-h-0">
          <ReactFlowProvider>
            <ActionMiniGraph pipeline={drillFlowgroup.pipeline} flowgroup={drillFlowgroup.name} />
          </ReactFlowProvider>
        </div>
      </div>
    </div>
  )
}
