import { Handle, Position, type NodeProps } from '@xyflow/react'

function CloudIcon() {
  return (
    <svg className="h-4 w-4 shrink-0 text-slate-400" viewBox="0 0 20 20" fill="currentColor">
      <path d="M5.5 4.4a6.9 6.9 0 0113 3.2h.3a4.3 4.3 0 01-.2 8.4H4.8a3.8 3.8 0 01-.6-7.5A6.9 6.9 0 015.5 4.4z" />
    </svg>
  )
}

export function ExternalNode({ data, selected }: NodeProps) {
  const label = (data.label as string) ?? ''
  const searchMatch = data.searchMatch as boolean | undefined
  const searchDimmed = data.searchDimmed as boolean | undefined

  return (
    <>
      <div className={`flex flex-col transition-opacity duration-200 ${searchDimmed ? 'opacity-20' : ''}`}>
        <span className="mb-1 text-[11px] text-slate-400">External</span>
        <div
          className={`flex items-center gap-2.5 rounded-md border border-dashed bg-white px-3 py-2 ${
            searchMatch
              ? 'ring-2 ring-blue-300/50 border-blue-400 shadow-md'
              : selected
                ? 'border-blue-400 shadow'
                : 'border-slate-300'
          }`}
          style={{ minWidth: 180 }}
        >
          <CloudIcon />
          <span className="truncate text-[13px] text-slate-500" style={{ maxWidth: 140 }}>
            {label}
          </span>
        </div>
      </div>
      <Handle type="source" position={Position.Right} className="!h-1 !w-1 !border-0 !bg-transparent" style={{ top: 'calc(50% + 9px)' }} />
    </>
  )
}
