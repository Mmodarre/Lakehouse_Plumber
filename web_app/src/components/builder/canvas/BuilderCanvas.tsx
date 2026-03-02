import { useCallback, useRef } from 'react'
import {
  ReactFlow,
  Controls,
  MiniMap,
  Background,
  type ReactFlowInstance,
} from '@xyflow/react'
import type { BuilderNode as BuilderNodeType, BuilderEdge as BuilderEdgeType } from '../types/builder'
import '@xyflow/react/dist/style.css'
import { BuilderActionNode } from './BuilderActionNode'
import { BuilderEdge } from './BuilderEdge'
import { useBuilderStore } from '../hooks/useBuilderStore'
import { ACTION_CATALOG } from '../hooks/useActionCatalog'
import type { ActionType, ActionSubtype, BuilderNode, ActionNodeConfig } from '../types/builder'
import { toViewName } from '../types/builder'

const nodeTypes = { builderAction: BuilderActionNode }
const edgeTypes = { builderEdge: BuilderEdge }

let nodeIdCounter = 0

export function BuilderCanvas() {
  const {
    nodes,
    edges,
    onNodesChange,
    onEdgesChange,
    onConnect,
    addNode,
    setSelectedNode,
  } = useBuilderStore()

  const reactFlowInstance = useRef<ReactFlowInstance<BuilderNodeType, BuilderEdgeType> | null>(null)

  const onInit = useCallback((instance: ReactFlowInstance<BuilderNodeType, BuilderEdgeType>) => {
    reactFlowInstance.current = instance
  }, [])

  const onDragOver = useCallback((event: React.DragEvent) => {
    event.preventDefault()
    event.dataTransfer.dropEffect = 'move'
  }, [])

  const onDrop = useCallback(
    (event: React.DragEvent) => {
      event.preventDefault()

      const actionType = event.dataTransfer.getData('application/lhp-action-type') as ActionType
      const actionSubtype = event.dataTransfer.getData('application/lhp-action-subtype') as ActionSubtype

      if (!actionType || !actionSubtype) return

      const rfInstance = reactFlowInstance.current
      if (!rfInstance) return

      const position = rfInstance.screenToFlowPosition({
        x: event.clientX,
        y: event.clientY,
      })

      const catalogEntry = ACTION_CATALOG.find(
        (e) => e.type === actionType && e.subtype === actionSubtype,
      )

      nodeIdCounter++
      const id = `action-${nodeIdCounter}`
      const defaultName = `${catalogEntry?.label ?? actionSubtype}_${nodeIdCounter}`

      const node: BuilderNode = {
        id,
        type: 'builderAction',
        position,
        data: {
          actionType,
          actionSubtype,
          actionName: defaultName,
          label: defaultName,
          isConfigured: false,
        },
      }

      const config: ActionNodeConfig = {
        id,
        actionName: defaultName,
        actionType,
        actionSubtype,
        target: actionType === 'write' ? '' : toViewName(defaultName),
        config: {},
        isYAMLMode: false,
      }

      addNode(node, config)
    },
    [addNode],
  )

  const onNodeClick = useCallback(
    (_: React.MouseEvent, node: { id: string }) => {
      setSelectedNode(node.id)
    },
    [setSelectedNode],
  )

  const onPaneClick = useCallback(() => {
    setSelectedNode(null)
  }, [setSelectedNode])

  return (
    <div className="h-full w-full">
      <ReactFlow
        nodes={nodes}
        edges={edges}
        onNodesChange={onNodesChange}
        onEdgesChange={onEdgesChange}
        onConnect={onConnect}
        onInit={onInit}
        onDragOver={onDragOver}
        onDrop={onDrop}
        onNodeClick={onNodeClick}
        onPaneClick={onPaneClick}
        nodeTypes={nodeTypes}
        edgeTypes={edgeTypes}
        defaultEdgeOptions={{ type: 'builderEdge' }}
        fitView
        className="bg-slate-50"
      >
        <Controls />
        <MiniMap />
        <Background />
      </ReactFlow>
    </div>
  )
}
