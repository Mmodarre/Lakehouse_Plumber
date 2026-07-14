import { Handle, Position, type NodeProps } from '@xyflow/react'
import { Boxes } from 'lucide-react'
import { NodeCard, NODE_HANDLE_CLASS } from './NodeCard'
import { NodeSeverityBadge } from './NodeSeverityBadge'
import { ExternalBadge } from '../badges/ExternalBadge'
import type { ExternalConnection } from '../../../types/graph'

export function FlowgroupNode({ data, selected }: NodeProps) {
  const label = (data.label as string) ?? ''
  const externalConnections = (data.externalConnections as ExternalConnection[]) ?? []
  const searchMatch = data.searchMatch as boolean | undefined
  const searchDimmed = data.searchDimmed as boolean | undefined
  // Optional enrichment (§6.7 G2): produced-dataset FQN hint + severity dot.
  // Both absent ⇒ renders exactly as before.
  const fqn = data.fqn as string | undefined

  return (
    <>
      <Handle type="target" position={Position.Left} className={NODE_HANDLE_CLASS} />
      <NodeCard
        label={label}
        sublabel={fqn ?? 'Flowgroup'}
        icon={Boxes}
        chipClassName="bg-node-flowgroup/12 text-node-flowgroup"
        selected={selected}
        searchMatch={searchMatch}
        searchDimmed={searchDimmed}
        className="w-75"
      >
        <NodeSeverityBadge severity={data.severity} />
        <ExternalBadge connections={externalConnections} />
      </NodeCard>
      <Handle type="source" position={Position.Right} className={NODE_HANDLE_CLASS} />
    </>
  )
}
