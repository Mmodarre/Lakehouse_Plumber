import { Handle, Position, type NodeProps } from '@xyflow/react'
import { Workflow } from 'lucide-react'
import { NodeCard, NODE_HANDLE_CLASS } from './NodeCard'
import { NodeSeverityBadge } from './NodeSeverityBadge'

export function PipelineNode({ data, selected }: NodeProps) {
  const label = (data.label as string) ?? ''
  const searchMatch = data.searchMatch as boolean | undefined
  const searchDimmed = data.searchDimmed as boolean | undefined
  // Optional enrichment (§6.7 G2): a severity dot flags validation issues.
  // Pipeline nodes carry NO sublabel — the enrichment FQN was a grey,
  // low-signal duplicate of the pipeline name.

  return (
    <>
      <Handle type="target" position={Position.Left} className={NODE_HANDLE_CLASS} />
      <NodeCard
        label={label}
        sublabel=""
        icon={Workflow}
        chipClassName="bg-node-pipeline/12 text-node-pipeline"
        selected={selected}
        searchMatch={searchMatch}
        searchDimmed={searchDimmed}
        className="max-w-60"
      >
        <NodeSeverityBadge severity={data.severity} />
      </NodeCard>
      <Handle type="source" position={Position.Right} className={NODE_HANDLE_CLASS} />
    </>
  )
}
