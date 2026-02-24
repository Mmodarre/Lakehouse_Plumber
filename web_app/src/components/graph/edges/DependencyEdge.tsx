import { getBezierPath, type EdgeProps } from '@xyflow/react'

const edgeStyles: Record<string, { stroke: string; strokeDasharray?: string; marker: string }> = {
  internal:        { stroke: '#94a3b8', marker: 'url(#arrow)' },
  cross_flowgroup: { stroke: '#94a3b8', strokeDasharray: '6 3', marker: 'url(#arrow)' },
  cross_pipeline:  { stroke: '#f59e0b', strokeDasharray: '3 3', marker: 'url(#arrow-cross-pipeline)' },
  external:        { stroke: '#cbd5e1', strokeDasharray: '6 3', marker: 'url(#arrow)' },
}

export function DependencyEdge({
  id,
  sourceX,
  sourceY,
  targetX,
  targetY,
  sourcePosition,
  targetPosition,
  data,
}: EdgeProps) {
  const [edgePath] = getBezierPath({
    sourceX,
    sourceY,
    targetX,
    targetY,
    sourcePosition,
    targetPosition,
  })

  const edgeType = (data?.edgeType as string) ?? 'internal'
  const style = edgeStyles[edgeType] ?? edgeStyles.internal

  const searchDimmed = data?.searchDimmed as boolean | undefined

  return (
    <g style={{
      opacity: searchDimmed ? 0.1 : 1,
      transition: 'opacity 200ms',
    }}>
      <path
        id={id}
        d={edgePath}
        fill="none"
        stroke={style.stroke}
        strokeWidth={1.2}
        strokeDasharray={style.strokeDasharray}
        markerEnd={style.marker}
      />
    </g>
  )
}
