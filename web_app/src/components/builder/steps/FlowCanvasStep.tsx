import { ReactFlowProvider } from '@xyflow/react'
import { ActionPalette } from '../canvas/ActionPalette'
import { BuilderCanvas } from '../canvas/BuilderCanvas'
import { ActionConfigDrawer } from '../config/ActionConfigDrawer'

export default function FlowCanvasStep() {
  return (
    <ReactFlowProvider>
      <div className="flex flex-1 overflow-hidden">
        <ActionPalette />
        <div className="flex-1">
          <BuilderCanvas />
        </div>
        <ActionConfigDrawer />
      </div>
    </ReactFlowProvider>
  )
}
