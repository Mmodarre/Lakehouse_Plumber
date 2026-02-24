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

const ACCENT_COLORS: Record<string, string> = {
  load: 'bg-blue-500',
  transform: 'bg-purple-500',
  write: 'bg-green-500',
  test: 'bg-amber-500',
}

function getTypeLabel(
  nodeType: string,
  sourceType?: string,
  transformType?: string,
  writeType?: string,
  testType?: string,
): string {
  switch (nodeType) {
    case 'load':
      return sourceType ? `Load (${sourceType})` : 'Load'
    case 'transform':
      return transformType ? `Transform (${transformType})` : 'Transform'
    case 'write':
      if (writeType === 'materialized_view') return 'Materialized view'
      if (writeType === 'sink') return 'Sink'
      return 'Streaming table'
    case 'test':
      return testType ? `Test (${testType})` : 'Test'
    default:
      return nodeType.charAt(0).toUpperCase() + nodeType.slice(1)
  }
}

function getWriteInfo(writeMode?: string, scdType?: number | string): string | null {
  const parts: string[] = []
  if (writeMode) parts.push(writeMode)
  if (scdType != null) parts.push(`SCD ${scdType}`)
  return parts.length > 0 ? parts.join(' · ') : null
}

export function ActionNode({ data, selected }: NodeProps) {
  const label = (data.label as string) ?? ''
  const nodeType = (data.nodeType as string) ?? 'load'
  const sourceType = data.source_type as string | undefined
  const transformType = data.transform_type as string | undefined
  const writeType = data.write_type as string | undefined
  const writeMode = data.write_mode as string | undefined
  const scdType = data.scd_type as number | string | undefined
  const testType = data.test_type as string | undefined
  const typeLabel = getTypeLabel(nodeType, sourceType, transformType, writeType, testType)
  const accentColor = ACCENT_COLORS[nodeType] ?? 'bg-slate-400'
  const writeInfo = nodeType === 'write' ? getWriteInfo(writeMode, scdType) : null

  return (
    <>
      <Handle type="target" position={Position.Left} className="!h-1 !w-1 !border-0 !bg-transparent" style={{ top: 'calc(50% + 9px)' }} />
      <div className="flex flex-col">
        {/* Type label above node */}
        <span className="mb-1 text-[11px] text-slate-400">{typeLabel}</span>

        {/* Node card */}
        <div
          className={`relative rounded-md border bg-white ${
            selected ? 'border-blue-400 shadow' : 'border-slate-200'
          }`}
          style={{ width: 240 }}
        >
          <div className={`absolute inset-x-0 top-0 h-[3px] rounded-t-md ${accentColor}`} />
          <div className="flex items-center gap-2.5 px-3 pt-3 pb-2">
            <DatasetIcon />
            <span className="truncate text-[13px] text-slate-800" title={label}>
              {label}
            </span>
          </div>

          {writeInfo && (
            <div className="border-t border-slate-100 px-3 py-1.5">
              <span className="text-[11px] text-slate-400">{writeInfo}</span>
            </div>
          )}
        </div>
      </div>
      <Handle type="source" position={Position.Right} className="!h-1 !w-1 !border-0 !bg-transparent" style={{ top: 'calc(50% + 9px)' }} />
    </>
  )
}
