import { ReactFlowProvider } from '@xyflow/react'
import { DependencyGraphWithControls } from '../../graph/DependencyGraph'
import { PipelineFilter } from './PipelineFilter'

// ── ProjectMapView — project-wide dependency map center tab (D11) ──
//
// A thin wrapper around graph/DependencyGraph (which owns the single decluttered
// toolbar row + its GraphControls/zoom/external-sources affordances). The only
// map-specific control composed here is the scope-aware pipeline picker, handed
// down as `scopePicker`. NO "Project map" title bar, NO medallion-layer lanes.

export function ProjectMapView() {
  return (
    <div className="flex h-full min-h-0 flex-col">
      <ReactFlowProvider>
        <DependencyGraphWithControls scopePicker={<PipelineFilter />} />
      </ReactFlowProvider>
    </div>
  )
}

export default ProjectMapView
