import { ReactFlowProvider } from '@xyflow/react'
import { X } from 'lucide-react'
import { useUIStore } from '../../store/uiStore'
import { useFlowgroupDetail } from '../../hooks/useFlowgroups'
import { Button } from '../ui/button'
import { Dialog, DialogContent, DialogTitle } from '../ui/dialog'
import { ActionMiniGraph } from './ActionMiniGraph'

export function FlowgroupModal() {
  const { drillFlowgroup, closeFlowgroupModal, openModal, openFlowgroupEditor } = useUIStore()
  const { data: detail } = useFlowgroupDetail(drillFlowgroup?.name ?? null)

  // Escape closes this dialog before the PipelineModal below it — Radix's
  // dismissable-layer stack sends Escape to the most recently opened layer.

  if (!drillFlowgroup) return null

  const sourceFile = detail?.source_file

  return (
    <Dialog
      open
      onOpenChange={(open) => {
        if (!open) closeFlowgroupModal()
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
              Flowgroup
            </span>
            <DialogTitle className="truncate text-base font-semibold text-foreground">
              {drillFlowgroup.name}
            </DialogTitle>
          </div>
          <div className="ml-3 flex shrink-0 items-center gap-2">
            <Button
              variant="outline"
              size="sm"
              disabled={!sourceFile}
              onClick={() => openFlowgroupEditor(drillFlowgroup.name, drillFlowgroup.pipeline)}
            >
              Edit Flowgroup
            </Button>
            <Button
              variant="outline"
              size="sm"
              onClick={() => openModal({ type: 'flowgroup', name: drillFlowgroup.name })}
            >
              View Config
            </Button>
            <Button
              variant="ghost"
              size="icon-sm"
              className="text-muted-foreground"
              aria-label="Close"
              onClick={closeFlowgroupModal}
            >
              <X aria-hidden="true" />
            </Button>
          </div>
        </div>

        {/* Body: action-level graph */}
        <div className="min-h-0 flex-1">
          <ReactFlowProvider>
            <ActionMiniGraph pipeline={drillFlowgroup.pipeline} flowgroup={drillFlowgroup.name} />
          </ReactFlowProvider>
        </div>
      </DialogContent>
    </Dialog>
  )
}
