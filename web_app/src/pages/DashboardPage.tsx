import { ReactFlowProvider } from '@xyflow/react'
import { DependencyGraphWithControls } from '../components/graph/DependencyGraph'
import { PipelineModal } from '../components/drill/PipelineModal'
import { FlowgroupModal } from '../components/drill/FlowgroupModal'

export function DashboardPage() {
  return (
    <div className="relative h-full">
      <ReactFlowProvider>
        <DependencyGraphWithControls />
      </ReactFlowProvider>
      <PipelineModal />
      <FlowgroupModal />
    </div>
  )
}
