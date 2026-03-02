import { useEffect, useRef, useState } from 'react'
import { ReactFlowProvider } from '@xyflow/react'
import '@xyflow/react/dist/style.css'
import { ActionPalette } from './canvas/ActionPalette'
import { BuilderCanvas } from './canvas/BuilderCanvas'
import { ActionConfigDrawer } from './config/ActionConfigDrawer'
import TemplateWizardStep from './steps/TemplateWizardStep'
import { useBuilderEditMode } from './hooks/useBuilderEditMode'
import { useBuilderStore } from './hooks/useBuilderStore'
import type { FlowgroupKind } from './hooks/useYAMLDeserializer'

interface VisualEditorTabProps {
  /** Current YAML from the code editor tab */
  yamlContent: string
  /** File path of the flowgroup YAML */
  filePath: string
  /** Flowgroup name (for multi-doc matching) */
  flowgroupName: string
  /** Pipeline name */
  pipeline: string
}

/**
 * Visual editor component that renders either a canvas-based editor
 * (for action-based flowgroups) or a template wizard (for template-based flowgroups).
 *
 * On mount, it deserializes the provided YAML into builder state.
 * The parent component can read the current builder state via useYAMLGenerator().
 */
export function VisualEditorTab({
  yamlContent,
  filePath,
  flowgroupName,
  pipeline: _pipeline,
}: VisualEditorTabProps) {
  const { loadFromYAML } = useBuilderEditMode()
  const chosenPath = useBuilderStore((s) => s.chosenPath)
  const editMode = useBuilderStore((s) => s.editMode)
  const [loadError, setLoadError] = useState<string | null>(null)
  const [isLoading, setIsLoading] = useState(true)
  const [kind, setKind] = useState<FlowgroupKind | null>(null)
  const loadRef = useRef<string | null>(null)

  // Load YAML into builder state on mount (or when content changes)
  useEffect(() => {
    // Skip if already loaded for this content
    const key = `${filePath}:${yamlContent.length}`
    if (loadRef.current === key) return
    loadRef.current = key

    setIsLoading(true)
    setLoadError(null)

    loadFromYAML(yamlContent, filePath, flowgroupName)
      .then((result) => {
        if (result.error) {
          setLoadError(result.error)
        }
        setKind(result.kind)
      })
      .catch((err) => {
        setLoadError(err instanceof Error ? err.message : 'Failed to load flowgroup')
      })
      .finally(() => {
        setIsLoading(false)
      })
    // Only run on mount — subsequent edits happen within the builder
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [])

  if (isLoading) {
    return (
      <div className="flex flex-1 items-center justify-center bg-white">
        <div className="flex items-center gap-3 text-sm text-slate-400">
          <div className="h-5 w-5 animate-spin rounded-full border-2 border-slate-300 border-t-slate-600" />
          Loading visual editor...
        </div>
      </div>
    )
  }

  if (loadError) {
    return (
      <div className="flex flex-1 flex-col items-center justify-center gap-3 bg-white p-8">
        <div className="text-sm font-medium text-red-600">Failed to load in visual mode</div>
        <p className="max-w-md text-center text-xs text-slate-500">{loadError}</p>
        <p className="text-xs text-slate-400">
          Switch back to the Code Editor to fix any YAML issues.
        </p>
      </div>
    )
  }

  // Template-based flowgroup
  if (kind === 'template' || chosenPath === 'template') {
    return (
      <div className="flex flex-1 flex-col overflow-hidden bg-white">
        {/* Read-only header showing edit context */}
        {editMode && (
          <div className="flex items-center gap-3 border-b border-slate-200 bg-slate-50 px-4 py-2">
            <span className="text-[10px] font-semibold uppercase tracking-wider text-slate-400">
              Editing Template Flowgroup
            </span>
          </div>
        )}
        <TemplateWizardStep />
      </div>
    )
  }

  // Canvas-based flowgroup
  return (
    <ReactFlowProvider>
      <div className="flex flex-1 overflow-hidden bg-white">
        <ActionPalette />
        <div className="flex-1">
          <BuilderCanvas />
        </div>
        <ActionConfigDrawer />
      </div>
    </ReactFlowProvider>
  )
}
