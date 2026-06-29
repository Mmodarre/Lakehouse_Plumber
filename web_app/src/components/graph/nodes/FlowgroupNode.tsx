import { Handle, Position, type NodeProps } from '@xyflow/react'
import { ExternalBadge } from '../badges/ExternalBadge'
import type { ExternalConnection } from '../../../types/graph'

function DatasetIcon() {
  return (
    <svg className="h-4 w-4 shrink-0" viewBox="0 0 20 20" fill="none">
      {/* Table header row (amber tint) */}
      <rect x="2" y="2" width="16" height="4" rx="1" fill="#e5a44c" opacity="0.6" />
      {/* Table body rows */}
      <rect x="2" y="7" width="7" height="3" rx="0.5" fill="#94a3b8" opacity="0.5" />
      <rect x="11" y="7" width="7" height="3" rx="0.5" fill="#94a3b8" opacity="0.35" />
      <rect x="2" y="11.5" width="7" height="3" rx="0.5" fill="#94a3b8" opacity="0.35" />
      <rect x="11" y="11.5" width="7" height="3" rx="0.5" fill="#94a3b8" opacity="0.5" />
      <rect x="2" y="16" width="7" height="2" rx="0.5" fill="#94a3b8" opacity="0.25" />
      <rect x="11" y="16" width="7" height="2" rx="0.5" fill="#94a3b8" opacity="0.25" />
    </svg>
  )
}

export function FlowgroupNode({ data, selected }: NodeProps) {
  const label = (data.label as string) ?? ''
  const externalConnections = (data.externalConnections as ExternalConnection[]) ?? []
  const searchMatch = data.searchMatch as boolean | undefined
  const searchDimmed = data.searchDimmed as boolean | undefined

  return (
    <>
      <Handle type="target" position={Position.Left} className="!h-1 !w-1 !border-0 !bg-transparent" style={{ top: 'calc(50% + 9px)' }} />
      <div className={`relative flex flex-col transition-opacity duration-200 ${searchDimmed ? 'opacity-20' : ''}`}>
        {/* Type label above node */}
        <span className="mb-1 text-[11px] text-slate-400">Flowgroup</span>

        {/* Node card */}
        <div
          className={`relative rounded-md border bg-white ${
            searchMatch
              ? 'ring-2 ring-blue-300/50 border-blue-400 shadow-md'
              : selected
                ? 'border-blue-400 shadow'
                : 'border-slate-200'
          }`}
          style={{ minWidth: 280 }}
        >
          <div className="flex items-center gap-2.5 px-3 py-2">
            <DatasetIcon />
            <span className="text-[13px] text-slate-800 break-words">
              {label}
            </span>
          </div>
          <ExternalBadge connections={externalConnections} />
        </div>
      </div>
      <Handle type="source" position={Position.Right} className="!h-1 !w-1 !border-0 !bg-transparent" style={{ top: 'calc(50% + 9px)' }} />
    </>
  )
}
