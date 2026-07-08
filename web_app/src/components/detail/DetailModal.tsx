import { useUIStore } from '../../store/uiStore'
import { Dialog, DialogContent, DialogTitle } from '../ui/dialog'
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

  if (!modalOpen || !selectedNode) return null

  return (
    <Dialog
      open
      onOpenChange={(open) => {
        if (!open) closeModal()
      }}
    >
      <DialogContent
        aria-describedby={undefined}
        className="flex max-h-[80vh] flex-col gap-0 overflow-hidden p-0 sm:max-w-2xl"
      >
        {/* Header */}
        <div className="border-b border-border px-6 py-3 pr-12">
          <span className="text-2xs font-semibold uppercase tracking-[0.05em] text-muted-foreground">
            {selectedNode.type}
          </span>
          <DialogTitle className="truncate text-base font-semibold text-foreground">
            {selectedNode.name}
          </DialogTitle>
        </div>

        {/* Body */}
        <div className="custom-scrollbar min-h-0 flex-1 overflow-y-auto px-6 py-4">
          <DetailContent />
        </div>
      </DialogContent>
    </Dialog>
  )
}
