import { lazy, Suspense, useEffect, useRef, useState } from 'react'
import { Save, Pencil, Eye } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { useBuilderStore } from '../hooks/useBuilderStore'
import { useYAMLGenerator } from '../hooks/useYAMLGenerator'
import { useSaveFlowgroup } from '../hooks/useSaveFlowgroup'
import type { MonacoEditorHandle } from '@/components/editor/MonacoEditorWrapper'

const MonacoEditorWrapper = lazy(
  () => import('@/components/editor/MonacoEditorWrapper'),
)

function EditorSkeleton() {
  return (
    <div className="flex flex-1 items-center justify-center bg-[#1e1e1e]">
      <div className="h-6 w-6 animate-spin rounded-full border-2 border-slate-500 border-t-slate-300" />
    </div>
  )
}

export default function PreviewSaveStep() {
  const {
    generatedYAML,
    editedYAML,
    setGeneratedYAML,
    setEditedYAML,
    isSaving,
    saveError,
    basicInfo,
  } = useBuilderStore()

  const yamlFromStore = useYAMLGenerator()
  const { save } = useSaveFlowgroup()
  const editorRef = useRef<MonacoEditorHandle>(null)

  const [isEditing, setIsEditing] = useState(false)

  // Update generated YAML when store changes
  useEffect(() => {
    if (yamlFromStore) {
      setGeneratedYAML(yamlFromStore)
    }
  }, [yamlFromStore, setGeneratedYAML])

  const displayContent = editedYAML ?? generatedYAML

  const activePipeline = basicInfo.isNewPipeline ? basicInfo.newPipeline : basicInfo.pipeline
  const activeSubdir = basicInfo.isNewSubdir ? basicInfo.newSubdir : basicInfo.subdirectory
  const filePath = (() => {
    if (!activePipeline || !basicInfo.flowgroupName) return 'preview.yaml'
    const parts = ['pipelines', activePipeline]
    if (activeSubdir && activeSubdir.trim()) parts.push(activeSubdir.trim())
    parts.push(`${basicInfo.flowgroupName}.yaml`)
    return parts.join('/')
  })()

  const handleSave = async () => {
    // If in editing mode, capture the editor content first
    if (isEditing && editorRef.current) {
      const editorContent = editorRef.current.getValue()
      setEditedYAML(editorContent)
    }
    await save()
  }

  return (
    <div className="flex flex-1 flex-col overflow-hidden">
      {/* Toolbar */}
      <div className="flex items-center justify-between border-b border-slate-200 bg-white px-6 py-3">
        <div>
          <h2 className="text-sm font-semibold text-slate-800">Preview & Save</h2>
          {filePath && (
            <p className="mt-0.5 font-mono text-xs text-slate-500">{filePath}</p>
          )}
        </div>
        <div className="flex items-center gap-2">
          <Button
            variant="outline"
            size="sm"
            onClick={() => {
              if (isEditing && editorRef.current) {
                setEditedYAML(editorRef.current.getValue())
              }
              setIsEditing(!isEditing)
            }}
            className="gap-1.5"
          >
            {isEditing ? (
              <>
                <Eye className="h-3 w-3" />
                Preview
              </>
            ) : (
              <>
                <Pencil className="h-3 w-3" />
                Edit
              </>
            )}
          </Button>
          <Button
            size="sm"
            onClick={handleSave}
            disabled={isSaving || !displayContent}
            className="gap-1.5"
          >
            <Save className="h-3 w-3" />
            {isSaving ? 'Saving...' : 'Create Flowgroup'}
          </Button>
        </div>
      </div>

      {saveError && (
        <div className="border-b border-red-200 bg-red-50 px-6 py-2 text-xs text-red-600">
          {saveError}
        </div>
      )}

      {/* Editor */}
      <div className="flex-1">
        <Suspense fallback={<EditorSkeleton />}>
          <MonacoEditorWrapper
            ref={editorRef}
            path={filePath}
            content={displayContent}
            readOnly={!isEditing}
            onDirtyChange={(dirty) => {
              if (dirty && isEditing && editorRef.current) {
                setEditedYAML(editorRef.current.getValue())
              }
            }}
          />
        </Suspense>
      </div>
    </div>
  )
}
