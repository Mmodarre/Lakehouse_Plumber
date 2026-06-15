import type { Node, Edge } from '@xyflow/react'

// ── Custom Node Data Types ───────────────────────────────

export interface BaseNodeData extends Record<string, unknown> {
  label: string
  pipeline: string
  stage: number
  metadata: Record<string, unknown>
}

export interface FlowgroupNodeData extends BaseNodeData {
  flowgroup: string
  actionCount: number
  actionTypes: string[]
  isStale?: boolean
}

export interface ActionNodeData extends BaseNodeData {
  flowgroup: string
  actionType: 'load' | 'transform' | 'write' | 'test'
  target?: string
}

export interface PipelineNodeData extends BaseNodeData {
  flowgroupCount: number
  actionCount: number
}

export interface ExternalNodeData extends BaseNodeData {
  sourceName: string
}

// ── React Flow Node/Edge Types ───────────────────────────

export type FlowgroupNode = Node<FlowgroupNodeData, 'flowgroup'>
export type ActionNode = Node<ActionNodeData, 'action'>
export type PipelineNode = Node<PipelineNodeData, 'pipeline'>
export type ExternalNode = Node<ExternalNodeData, 'external'>

export type AppNode = FlowgroupNode | ActionNode | PipelineNode | ExternalNode
export type AppEdge = Edge & {
  data?: {
    edgeType: 'internal' | 'cross_flowgroup' | 'cross_pipeline' | 'external'
  }
}

// ── External Connection Types ────────────────────────────

export interface ExternalConnection {
  direction: 'upstream' | 'downstream'
  targetNodeId: string
  targetPipeline: string
}

// ── ELK Layout Types ─────────────────────────────────────

export interface ElkLayoutOptions {
  direction: 'RIGHT' | 'DOWN'
  nodeSpacingH: number
  nodeSpacingV: number
  edgeRouting: 'ORTHOGONAL' | 'POLYLINE'
}
