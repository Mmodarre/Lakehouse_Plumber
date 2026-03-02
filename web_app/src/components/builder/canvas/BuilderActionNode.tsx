import { memo } from 'react'
import { Handle, Position, type NodeProps } from '@xyflow/react'
import type { BuilderNodeData } from '../types/builder'
import { ACTION_TYPE_COLORS } from '../hooks/useActionCatalog'
import { ACTION_CATALOG } from '../hooks/useActionCatalog'

function BuilderActionNodeInner({ data, selected }: NodeProps) {
  const nodeData = data as unknown as BuilderNodeData
  const colors = ACTION_TYPE_COLORS[nodeData.actionType]
  const catalogEntry = ACTION_CATALOG.find(
    (e) => e.type === nodeData.actionType && e.subtype === nodeData.actionSubtype,
  )

  return (
    <div
      className={`min-w-[160px] rounded-lg border-2 bg-white shadow-sm transition-shadow ${
        selected ? 'shadow-md ring-2 ring-blue-300' : 'hover:shadow-md'
      } ${colors.border}`}
    >
      {/* Color bar */}
      <div className={`h-1.5 rounded-t-[5px] ${colors.bg}`} />

      <div className="px-3 py-2">
        <div className={`text-[10px] font-semibold uppercase tracking-wider ${colors.text}`}>
          {catalogEntry?.label ?? nodeData.actionSubtype}
        </div>
        <div className="mt-0.5 text-xs font-medium text-slate-700">
          {nodeData.actionName || 'Untitled'}
        </div>
        {!nodeData.isConfigured && (
          <div className="mt-1 text-[10px] text-amber-500">Click to configure</div>
        )}
      </div>

      {/* Handles */}
      <Handle
        type="target"
        position={Position.Left}
        className="!h-3 !w-3 !rounded-full !border-2 !border-slate-300 !bg-white !opacity-100"
      />
      <Handle
        type="source"
        position={Position.Right}
        className="!h-3 !w-3 !rounded-full !border-2 !border-slate-300 !bg-white !opacity-100"
      />
    </div>
  )
}

export const BuilderActionNode = memo(BuilderActionNodeInner)
