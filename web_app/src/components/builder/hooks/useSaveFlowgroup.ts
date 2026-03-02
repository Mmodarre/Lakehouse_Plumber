import { useCallback } from 'react'
import { useQueryClient } from '@tanstack/react-query'
import { toast } from 'sonner'
import { writeFile } from '@/api/files'
import { useBuilderStore } from './useBuilderStore'
import { useUIStore } from '@/store/uiStore'

export function useSaveFlowgroup() {
  const queryClient = useQueryClient()
  const { basicInfo, editedYAML, generatedYAML, setSaving, setSaveError, reset } =
    useBuilderStore()
  const closeBuilder = useUIStore((s) => s.closeFlowgroupBuilder)

  const save = useCallback(async () => {
    const yamlContent = editedYAML ?? generatedYAML
    if (!yamlContent) {
      setSaveError('No YAML content to save')
      return
    }

    const activePipeline = basicInfo.isNewPipeline ? basicInfo.newPipeline : basicInfo.pipeline
    if (!activePipeline || !basicInfo.flowgroupName) {
      setSaveError('Pipeline and flowgroup name are required')
      return
    }

    // Build file path
    const activeSubdir = basicInfo.isNewSubdir ? basicInfo.newSubdir : basicInfo.subdirectory
    const parts = ['pipelines', activePipeline]
    if (activeSubdir && activeSubdir.trim()) parts.push(activeSubdir.trim())
    parts.push(`${basicInfo.flowgroupName}.yaml`)
    const filePath = parts.join('/')

    setSaving(true)
    setSaveError(null)

    try {
      await writeFile(filePath, yamlContent)

      // Invalidate all related queries
      await Promise.all([
        queryClient.invalidateQueries({ queryKey: ['flowgroups'] }),
        queryClient.invalidateQueries({ queryKey: ['pipelines'] }),
        queryClient.invalidateQueries({ queryKey: ['dep-graph'] }),
        queryClient.invalidateQueries({ queryKey: ['files'] }),
        queryClient.invalidateQueries({ queryKey: ['git-status'] }),
      ])

      toast.success(`Created flowgroup "${basicInfo.flowgroupName}"`)
      reset()
      closeBuilder()
    } catch (err) {
      const message = err instanceof Error ? err.message : 'Failed to save flowgroup'
      setSaveError(message)
      toast.error(message)
    } finally {
      setSaving(false)
    }
  }, [
    editedYAML,
    generatedYAML,
    basicInfo,
    setSaving,
    setSaveError,
    queryClient,
    reset,
    closeBuilder,
  ])

  return { save }
}
