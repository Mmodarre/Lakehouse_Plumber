import { ReactFlowProvider } from '@xyflow/react'
import { X } from 'lucide-react'
import { useUIStore } from '../../store/uiStore'
import { Button } from '../ui/button'
import { Dialog, DialogContent, DialogTitle } from '../ui/dialog'
import { FlowgroupMiniGraph } from './FlowgroupMiniGraph'

export function PipelineModal() {
  const { drillPipeline, closePipelineModal, openModal } = useUIStore()

  // Escape cascade (flowgroup first, then pipeline) is handled by Radix's
  // dismissable-layer stack: the FlowgroupModal dialog opens later, so it sits
  // on the higher layer and receives Escape before this one.

  if (!drillPipeline) return null

  return (
    <Dialog
      open
      onOpenChange={(open) => {
        if (!open) closePipelineModal()
      }}
    >
      <DialogContent
        showCloseButton={false}
        aria-describedby={undefined}
        className="flex h-[70vh] flex-col gap-0 overflow-hidden p-0 sm:max-w-4xl"
      >
        {/* Header */}
        <div className="flex items-center justify-between border-b border-border px-5 py-3">
          <div className="min-w-0">
            <span className="text-2xs font-semibold uppercase tracking-[0.05em] text-muted-foreground">
              Pipeline
            </span>
            <DialogTitle className="truncate text-base font-semibold text-foreground">
              {drillPipeline}
            </DialogTitle>
          </div>
          <div className="ml-3 flex shrink-0 items-center gap-2">
            <Button
              variant="outline"
              size="sm"
              onClick={() => openModal({ type: 'pipeline', name: drillPipeline })}
            >
              View Config
            </Button>
            <Button
              variant="ghost"
              size="icon-sm"
              className="text-muted-foreground"
              aria-label="Close"
              onClick={closePipelineModal}
            >
              <X aria-hidden="true" />
            </Button>
          </div>
        </div>

        {/* Body: mini flowgroup graph */}
        <div className="min-h-0 flex-1">
          <ReactFlowProvider>
            <FlowgroupMiniGraph pipeline={drillPipeline} />
          </ReactFlowProvider>
        </div>
      </DialogContent>
    </Dialog>
  )
}
