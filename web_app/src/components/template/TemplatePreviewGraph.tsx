import { useMemo } from 'react'
import { Background, Controls, ReactFlow, type Node } from '@xyflow/react'
import { stringify } from 'yaml'
import { deriveGraph, parseFlowgroupFile, selectTemplate } from '@/lib/flowgroup-doc'
import { toDesignerGraph, EMPTY_DESIGNER_GRAPH } from '@/components/designer/designerGraph'
import { useElkLayout } from '@/components/graph/useElkLayout'

export function TemplatePreviewGraph({ actions }: { actions: Record<string, unknown>[] }) {
  const graph = useMemo(() => {
    const doc = selectTemplate(parseFlowgroupFile(stringify({ name: 'preview', parameters: [], actions })))
    return doc ? toDesignerGraph(deriveGraph(doc.body), '', 'preview') : EMPTY_DESIGNER_GRAPH
  }, [actions])
  const { nodes: laidOut, edges, isLayouting } = useElkLayout(graph.nodes, graph.edges)
  const nodes = useMemo<Node[]>(() => laidOut.map((node) => ({ ...node, type: 'default', data: { label: String(node.data.label) }, className: '!border-border !bg-card !text-foreground', draggable: false, connectable: false })), [laidOut])
  return <div className="h-80 rounded-md border border-border" aria-label="Read-only preview action graph">
    {isLayouting ? <p role="status" className="p-3 text-xs">Laying out actions…</p> : <ReactFlow nodes={nodes} edges={edges.map((edge) => ({ ...edge, type: 'default' }))} fitView nodesDraggable={false} nodesConnectable={false} deleteKeyCode={null} proOptions={{ hideAttribution: true }}><Background /><Controls showInteractive={false} /></ReactFlow>}
  </div>
}
