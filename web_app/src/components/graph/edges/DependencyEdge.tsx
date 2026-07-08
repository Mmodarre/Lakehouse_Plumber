import { getBezierPath, type EdgeProps } from '@xyflow/react'

interface EdgeStyle {
  stroke: string
  strokeDasharray?: string
  strokeOpacity?: number
  marker: string
}

const edgeStyles: Record<string, EdgeStyle> = {
  internal:        { stroke: 'var(--edge)', marker: 'url(#lhp-arrow)' },
  cross_flowgroup: { stroke: 'var(--edge)', strokeDasharray: '6 3', marker: 'url(#lhp-arrow)' },
  cross_pipeline:  { stroke: 'var(--edge-cross)', strokeDasharray: '4 3', marker: 'url(#lhp-arrow-cross)' },
  external:        { stroke: 'var(--edge)', strokeDasharray: '6 3', strokeOpacity: 0.6, marker: 'url(#lhp-arrow)' },
}

/**
 * Arrowhead defs shared by every canvas that renders DependencyEdge.
 * Mount once inside each <ReactFlow> so `url(#...)` refs resolve locally.
 */
export function EdgeMarkerDefs() {
  return (
    <svg aria-hidden="true">
      <defs>
        <marker
          id="lhp-arrow"
          viewBox="0 0 10 10"
          refX="9"
          refY="5"
          markerWidth="7"
          markerHeight="7"
          orient="auto-start-reverse"
        >
          <path d="M 0 0 L 10 5 L 0 10 z" style={{ fill: 'var(--edge)' }} />
        </marker>
        <marker
          id="lhp-arrow-cross"
          viewBox="0 0 10 10"
          refX="9"
          refY="5"
          markerWidth="7"
          markerHeight="7"
          orient="auto-start-reverse"
        >
          <path d="M 0 0 L 10 5 L 0 10 z" style={{ fill: 'var(--edge-cross)' }} />
        </marker>
        <marker
          id="lhp-arrow-selected"
          viewBox="0 0 10 10"
          refX="9"
          refY="5"
          markerWidth="7"
          markerHeight="7"
          orient="auto-start-reverse"
        >
          <path d="M 0 0 L 10 5 L 0 10 z" style={{ fill: 'var(--primary)' }} />
        </marker>
      </defs>
    </svg>
  )
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
  selected,
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
        style={{
          stroke: selected ? 'var(--primary)' : style.stroke,
          strokeOpacity: selected ? 1 : style.strokeOpacity,
        }}
        strokeWidth={selected ? 2 : 1.5}
        strokeDasharray={style.strokeDasharray}
        markerEnd={selected ? 'url(#lhp-arrow-selected)' : style.marker}
      />
    </g>
  )
}
