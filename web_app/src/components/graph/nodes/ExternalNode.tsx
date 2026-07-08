import { Handle, Position, type NodeProps } from '@xyflow/react'
import { Cloud } from 'lucide-react'
import { NodeCard, NODE_HANDLE_CLASS } from './NodeCard'

export function ExternalNode({ data, selected }: NodeProps) {
  const label = (data.label as string) ?? ''
  const searchMatch = data.searchMatch as boolean | undefined
  const searchDimmed = data.searchDimmed as boolean | undefined

  return (
    <>
      <NodeCard
        label={label}
        sublabel="External source"
        icon={Cloud}
        chipClassName="bg-node-external/12 text-node-external"
        selected={selected}
        searchMatch={searchMatch}
        searchDimmed={searchDimmed}
        dashed
        muted
        className="max-w-60"
      />
      <Handle type="source" position={Position.Right} className={NODE_HANDLE_CLASS} />
    </>
  )
}
