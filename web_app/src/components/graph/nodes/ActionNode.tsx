import { Handle, Position, type NodeProps } from '@xyflow/react'
import { NodeCard, NODE_HANDLE_CLASS } from './NodeCard'
import { KIND_STYLES, FALLBACK_KIND_STYLE } from './kindStyles'

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
  const searchMatch = data.searchMatch as boolean | undefined
  const searchDimmed = data.searchDimmed as boolean | undefined

  const typeLabel = getTypeLabel(nodeType, sourceType, transformType, writeType, testType)
  const writeInfo = nodeType === 'write' ? getWriteInfo(writeMode, scdType) : null
  const kind = KIND_STYLES[nodeType] ?? FALLBACK_KIND_STYLE

  return (
    <>
      <Handle type="target" position={Position.Left} className={NODE_HANDLE_CLASS} />
      <NodeCard
        label={label}
        sublabel={writeInfo ? `${typeLabel} · ${writeInfo}` : typeLabel}
        icon={kind.icon}
        chipClassName={kind.chip}
        selected={selected}
        searchMatch={searchMatch}
        searchDimmed={searchDimmed}
        className="w-60"
      />
      <Handle type="source" position={Position.Right} className={NODE_HANDLE_CLASS} />
    </>
  )
}
