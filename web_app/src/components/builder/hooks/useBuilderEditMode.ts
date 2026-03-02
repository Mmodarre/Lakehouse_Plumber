import { useCallback } from 'react'
import { useBuilderStore } from './useBuilderStore'
import { deserializeFlowgroup, type FlowgroupKind } from './useYAMLDeserializer'
import { computeBuilderLayout } from './useBuilderElkLayout'
import type { BuilderNode, BuilderPath } from '../types/builder'

interface LoadResult {
  kind: FlowgroupKind
  error?: string
}

/**
 * Hook that orchestrates loading a YAML flowgroup into the builder store for editing.
 *
 * Ties together: YAML deserialization → ELK layout → store population.
 */
export function useBuilderEditMode() {
  const { loadState, setEditMode, reset } = useBuilderStore()

  const loadFromYAML = useCallback(
    async (
      yamlContent: string,
      filePath: string,
      flowgroupName?: string,
    ): Promise<LoadResult> => {
      // 1. Reset any existing builder state
      reset()

      // 2. Parse and deserialize
      const result = deserializeFlowgroup(yamlContent, flowgroupName)
      if (!result) {
        return { kind: 'unknown', error: 'Failed to parse YAML content' }
      }

      const { kind, basicInfo, templateInfo, actionConfigs: configs, edges: deserializedEdges } = result

      // 3. Determine builder path
      let chosenPath: BuilderPath = null
      if (kind === 'template') {
        chosenPath = 'template'
      } else if (kind === 'canvas' || kind === 'unknown') {
        chosenPath = 'canvas'
      }

      // 4. For template mode, just load basic + template info
      if (chosenPath === 'template') {
        const configMap = new Map<string, import('../types/builder').ActionNodeConfig>()
        loadState({
          basicInfo: basicInfo,
          templateInfo: templateInfo,
          chosenPath: 'template',
          nodes: [],
          edges: [],
          actionConfigs: configMap,
        })
        setEditMode({ isEdit: true, filePath, originalYAML: yamlContent })
        return { kind }
      }

      // 5. For canvas mode: compute layout, build nodes
      const actionConfigsList = configs ?? []
      const edges = deserializedEdges ?? []

      // Compute ELK positions
      const positions = await computeBuilderLayout(actionConfigsList, edges)

      // Create BuilderNode per action with positions
      const nodes: BuilderNode[] = actionConfigsList.map((config) => {
        const pos = positions.get(config.id) ?? { x: 0, y: 0 }
        return {
          id: config.id,
          type: 'builderAction' as const,
          position: pos,
          data: {
            actionType: config.actionType,
            actionSubtype: config.actionSubtype,
            actionName: config.actionName,
            label: config.actionName,
            isConfigured: Object.keys(config.config).length > 0 || config.isYAMLMode,
          },
        }
      })

      // Build actionConfigs map
      const configMap = new Map<string, import('../types/builder').ActionNodeConfig>()
      for (const config of actionConfigsList) {
        configMap.set(config.id, config)
      }

      // 6. Bulk-load into store
      loadState({
        basicInfo: basicInfo,
        templateInfo: undefined,
        chosenPath: 'canvas',
        nodes,
        edges,
        actionConfigs: configMap,
      })

      // 7. Set edit mode metadata
      setEditMode({ isEdit: true, filePath, originalYAML: yamlContent })

      return { kind }
    },
    [loadState, setEditMode, reset],
  )

  return { loadFromYAML }
}
