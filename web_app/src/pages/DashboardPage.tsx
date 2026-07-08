import { ReactFlowProvider } from '@xyflow/react'
import { DependencyGraphWithControls } from '../components/graph/DependencyGraph'
import { PipelineModal } from '../components/drill/PipelineModal'
import { FlowgroupModal } from '../components/drill/FlowgroupModal'
import { ErrorBoundary } from '../components/common/ErrorBoundary'
import { ModalErrorFallback } from '../components/common/ModalErrorFallback'
import { useUIStore } from '../store/uiStore'

export function DashboardPage() {
  const drillPipeline = useUIStore((s) => s.drillPipeline)
  const closePipelineModal = useUIStore((s) => s.closePipelineModal)
  const drillFlowgroup = useUIStore((s) => s.drillFlowgroup)
  const closeFlowgroupModal = useUIStore((s) => s.closeFlowgroupModal)

  return (
    <div className="relative h-full">
      <ReactFlowProvider>
        <DependencyGraphWithControls />
      </ReactFlowProvider>
      <ErrorBoundary
        fallback={<ModalErrorFallback onClose={closePipelineModal} />}
        resetKeys={[drillPipeline]}
      >
        <PipelineModal />
      </ErrorBoundary>
      <ErrorBoundary
        fallback={<ModalErrorFallback onClose={closeFlowgroupModal} />}
        resetKeys={[drillFlowgroup]}
      >
        <FlowgroupModal />
      </ErrorBoundary>
    </div>
  )
}
