import { useEffect } from 'react'
import { useUIStore } from '../../store/uiStore'
import { FlowgroupDetail } from './FlowgroupDetail'
import { PresetDetail } from './PresetDetail'
import { TemplateDetail } from './TemplateDetail'
import { PipelineDetail } from './PipelineDetail'

function DetailContent() {
  const selectedNode = useUIStore((s) => s.selectedNode)

  if (!selectedNode) return null

  switch (selectedNode.type) {
    case 'flowgroup':
      return <FlowgroupDetail name={selectedNode.name} />
    case 'preset':
      return <PresetDetail name={selectedNode.name} />
    case 'template':
      return <TemplateDetail name={selectedNode.name} />
    case 'pipeline':
      return <PipelineDetail name={selectedNode.name} />
  }
}

export function DetailModal() {
  const { modalOpen, closeModal, selectedNode } = useUIStore()

  // Close on Escape
  useEffect(() => {
    const handleKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape' && modalOpen) closeModal()
    }
    document.addEventListener('keydown', handleKey)
    return () => document.removeEventListener('keydown', handleKey)
  }, [modalOpen, closeModal])

  if (!modalOpen || !selectedNode) return null

  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-black/30"
      onClick={(e) => {
        if (e.target === e.currentTarget) closeModal()
      }}
    >
      <div className="max-h-[80vh] w-full max-w-2xl overflow-hidden rounded-lg border border-slate-200 bg-white shadow-xl">
        {/* Header */}
        <div className="flex items-center justify-between border-b border-slate-200 px-6 py-3">
          <div>
            <span className="text-[10px] font-semibold uppercase tracking-wider text-slate-400">
              {selectedNode.type}
            </span>
            <h2 className="text-base font-semibold text-slate-800">
              {selectedNode.name}
            </h2>
          </div>
          <button
            onClick={closeModal}
            className="rounded p-1.5 text-slate-400 hover:bg-slate-100 hover:text-slate-600"
          >
            <svg className="h-5 w-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
            </svg>
          </button>
        </div>

        {/* Body */}
        <div className="custom-scrollbar max-h-[calc(80vh-60px)] overflow-y-auto px-6 py-4">
          <DetailContent />
        </div>
      </div>
    </div>
  )
}
