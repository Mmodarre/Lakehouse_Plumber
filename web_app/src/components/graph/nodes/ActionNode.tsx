import { Handle, Position, type NodeProps } from '@xyflow/react'
import {
  ArrowDownToLine,
  CircleHelp,
  Database,
  FlaskConical,
  Wand2,
  type LucideIcon,
} from 'lucide-react'
import { NodeCard, NODE_HANDLE_CLASS } from './NodeCard'

// Kind identity resolves to the shared --kind-* tokens (same palette as the
// table badges) — chip tint always paired with a distinct icon.
const KIND_STYLES: Record<string, { chip: string; icon: LucideIcon }> = {
  load: { chip: 'bg-kind-load/12 text-kind-load', icon: ArrowDownToLine },
  transform: { chip: 'bg-kind-transform/12 text-kind-transform', icon: Wand2 },
  write: { chip: 'bg-kind-write/12 text-kind-write', icon: Database },
  test: { chip: 'bg-kind-test/12 text-kind-test', icon: FlaskConical },
}

const FALLBACK_STYLE = { chip: 'bg-muted text-muted-foreground', icon: CircleHelp }

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
  const kind = KIND_STYLES[nodeType] ?? FALLBACK_STYLE

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
