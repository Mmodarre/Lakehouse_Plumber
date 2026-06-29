import { useEffect } from 'react'
import { ReactFlowProvider } from '@xyflow/react'
import { useUIStore } from '../../store/uiStore'
import { FlowgroupMiniGraph } from './FlowgroupMiniGraph'

export function PipelineModal() {
  const { drillPipeline, closePipelineModal, drillFlowgroup, closeFlowgroupModal, openModal } = useUIStore()

  useEffect(() => {
    const handleKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape') {
        if (drillFlowgroup) {
          closeFlowgroupModal()
        } else if (drillPipeline) {
          closePipelineModal()
        }
      }
    }
    document.addEventListener('keydown', handleKey)
    return () => document.removeEventListener('keydown', handleKey)
  }, [drillPipeline, drillFlowgroup, closePipelineModal, closeFlowgroupModal])

  if (!drillPipeline) return null

  return (
    <div
      className="fixed inset-0 z-40 flex items-center justify-center bg-black/25"
      onClick={(e) => {
        if (e.target === e.currentTarget) closePipelineModal()
      }}
    >
      <div className="flex h-[70vh] w-full max-w-4xl flex-col overflow-hidden rounded-lg border border-slate-200 bg-white shadow-xl">
        {/* Header */}
        <div className="flex items-center justify-between border-b border-slate-200 px-5 py-3">
          <div>
            <span className="text-[10px] font-semibold uppercase tracking-wider text-slate-400">
              Pipeline
            </span>
            <h2 className="text-base font-semibold text-slate-800">{drillPipeline}</h2>
          </div>
          <div className="flex items-center gap-2">
            <button
              className="rounded bg-slate-100 px-3 py-1.5 text-[11px] font-medium text-slate-700 hover:bg-slate-200"
              onClick={() => openModal({ type: 'pipeline', name: drillPipeline })}
            >
              View Config
            </button>
            <button
              onClick={closePipelineModal}
              className="rounded p-1.5 text-slate-400 hover:bg-slate-100 hover:text-slate-600"
            >
              <svg className="h-5 w-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
              </svg>
            </button>
          </div>
        </div>

        {/* Body: mini flowgroup graph */}
        <div className="flex-1 min-h-0">
          <ReactFlowProvider>
            <FlowgroupMiniGraph pipeline={drillPipeline} />
          </ReactFlowProvider>
        </div>
      </div>
    </div>
  )
}
