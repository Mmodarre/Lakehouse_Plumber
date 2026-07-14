import { useCallback, useMemo } from 'react'
import { ReactFlowProvider, type Node } from '@xyflow/react'

import { GraphCanvas } from '../../graph/DependencyGraph'
import { useElkLayout } from '../../graph/useElkLayout'
import { useDependencyGraph } from '../../../hooks/useDependencyGraph'
import { useMapEnrichment } from '../../../hooks/useMapEnrichment'
import { useFlowgroups } from '../../../hooks/useFlowgroups'
import { useUIStore } from '../../../store/uiStore'
import { useWorkspaceStore } from '../../../store/workspaceStore'
import { LoadingSpinner } from '../../common/LoadingSpinner'
import { EmptyState } from '../../common/EmptyState'

// ── PipelineDagView — one pipeline's flowgroup-level DAG (drill target) ──
//
// Clicking a pipeline node on the project map opens this tab. It renders the
// SAME graph machinery the project map uses (GraphCanvas + useElkLayout +
// useMapEnrichment), sourced from the per-pipeline flowgroup graph
// (useDependencyGraph('flowgroup', pipeline)). Clicking a flowgroup node
// resolves its source_file from the flowgroups list and opens/focuses its
// entity tab — the single-click drill mirrors the map's flowgroup activation.

function PipelineDagView({ pipeline }: { pipeline: string }) {
  const { data, isLoading, error } = useDependencyGraph('flowgroup', pipeline)
  const openEntityTab = useWorkspaceStore((s) => s.openEntityTab)
  const { data: flowgroupData } = useFlowgroups()
  const selectedEnv = useUIStore((s) => s.selectedEnv)
  const enrichment = useMapEnrichment(selectedEnv)

  const apiNodes = useMemo(() => data?.nodes ?? [], [data])
  const apiEdges = useMemo(() => data?.edges ?? [], [data])
  const { nodes: layoutNodes, edges: layoutEdges, isLayouting } = useElkLayout(apiNodes, apiEdges)

  // Client-side severity + produced-FQN join (§6.7 G2), identical to the map:
  // purely additive to node `data`, so an un-enriched node renders unchanged.
  const enrichedNodes = useMemo(
    () =>
      layoutNodes.map((n) => {
        const e = enrichment.enrichNode({
          nodeType: n.data.nodeType as string | undefined,
          pipeline: n.data.pipeline as string | undefined,
          flowgroup: n.data.flowgroup as string | undefined,
        })
        if (!e.severity && !e.fqn) return n
        return { ...n, data: { ...n.data, severity: e.severity, fqn: e.fqn } }
      }),
    [layoutNodes, enrichment],
  )

  // Clicking a flowgroup node opens/focuses its entity tab. The graph node
  // carries pipeline + flowgroup but not the file path, so resolve source_file
  // from the flowgroups list (mirrors DependencyGraph's flowgroup activation).
  const onNodeClick = useCallback(
    (_: React.MouseEvent, node: Node) => {
      if (node.type !== 'flowgroup') return
      const flowgroup = typeof node.data.flowgroup === 'string' ? node.data.flowgroup : ''
      if (flowgroup === '') return
      const summary = flowgroupData?.flowgroups.find(
        (f) => f.name === flowgroup && f.pipeline === pipeline,
      )
      if (!summary?.source_file) return
      openEntityTab(summary.pipeline, summary.name, summary.source_file)
    },
    [openEntityTab, flowgroupData, pipeline],
  )

  // Layout runs asynchronously in a worker: keep the spinner up until the fetch
  // resolves AND the fetched nodes have been laid out (avoids an empty-canvas
  // flash between data arrival and the first ELK pass).
  const laidOut = layoutNodes.length > 0
  if (isLoading || isLayouting || (apiNodes.length > 0 && !laidOut)) {
    return <LoadingSpinner className="h-full" />
  }

  if (error) {
    return (
      <div className="flex h-full items-center justify-center text-sm text-destructive">
        Failed to load graph: {error.message}
      </div>
    )
  }

  if (apiNodes.length === 0) {
    return (
      <EmptyState
        title="No flowgroups"
        message={`Pipeline "${pipeline}" has no flowgroups to graph.`}
      />
    )
  }

  return (
    <div className="flex h-full min-h-0 flex-col">
      <ReactFlowProvider>
        <GraphCanvas nodes={enrichedNodes} edges={layoutEdges} onNodeClick={onNodeClick} />
      </ReactFlowProvider>
    </div>
  )
}

export { PipelineDagView }
export default PipelineDagView
