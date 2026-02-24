import { Handle, Position, type NodeProps } from '@xyflow/react'

function DatasetIcon() {
  return (
    <svg className="h-4 w-4 shrink-0" viewBox="0 0 20 20" fill="none">
      <rect x="2" y="2" width="16" height="4" rx="1" fill="#e5a44c" opacity="0.6" />
      <rect x="2" y="7" width="7" height="3" rx="0.5" fill="#94a3b8" opacity="0.5" />
      <rect x="11" y="7" width="7" height="3" rx="0.5" fill="#94a3b8" opacity="0.35" />
      <rect x="2" y="11.5" width="7" height="3" rx="0.5" fill="#94a3b8" opacity="0.35" />
      <rect x="11" y="11.5" width="7" height="3" rx="0.5" fill="#94a3b8" opacity="0.5" />
      <rect x="2" y="16" width="7" height="2" rx="0.5" fill="#94a3b8" opacity="0.25" />
      <rect x="11" y="16" width="7" height="2" rx="0.5" fill="#94a3b8" opacity="0.25" />
    </svg>
  )
}

export function PipelineNode({ data, selected }: NodeProps) {
  const label = (data.label as string) ?? ''
  const searchMatch = data.searchMatch as boolean | undefined
  const searchDimmed = data.searchDimmed as boolean | undefined

  return (
    <>
      <Handle type="target" position={Position.Left} className="!h-1 !w-1 !border-0 !bg-transparent" style={{ top: 'calc(50% + 9px)' }} />
      <div className={`flex flex-col transition-opacity duration-200 ${searchDimmed ? 'opacity-20' : ''}`}>
        <span className="mb-1 text-[11px] text-slate-400">Pipeline</span>
        <div
          className={`flex items-center gap-2.5 rounded-md border bg-white px-3 py-2 ${
            searchMatch
              ? 'ring-2 ring-blue-300/50 border-blue-400 shadow-md'
              : selected
                ? 'border-blue-400 shadow'
                : 'border-slate-200'
          }`}
          style={{ minWidth: 180 }}
        >
          <DatasetIcon />
          <span className="truncate text-[13px] text-slate-800" style={{ maxWidth: 140 }}>
            {label}
          </span>
        </div>
      </div>
      <Handle type="source" position={Position.Right} className="!h-1 !w-1 !border-0 !bg-transparent" style={{ top: 'calc(50% + 9px)' }} />
    </>
  )
}
