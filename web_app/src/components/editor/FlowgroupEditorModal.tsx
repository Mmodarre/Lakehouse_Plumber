import { lazy, Suspense, useEffect, useRef, useState, useCallback } from 'react'
import { useQueryClient } from '@tanstack/react-query'
import { toast } from 'sonner'
import { useUIStore } from '../../store/uiStore'
import { useFlowgroupRelatedFiles } from '../../hooks/useFlowgroups'
import { fetchFilePath, writeFile } from '../../api/files'
import { ApiError } from '../../api/client'
import type { MonacoEditorHandle } from './MonacoEditorWrapper'
import type { RelatedFileInfo } from '../../types/api'

const MonacoEditorWrapper = lazy(() => import('./MonacoEditorWrapper'))

interface TabState {
  path: string
  category: string // 'yaml' | 'sql' | 'python' | 'schema' | 'expectations'
  content: string
  originalContent: string
  isDirty: boolean
  isSaving: boolean
  exists: boolean
  isNew: boolean // true = created from a non-existent reference
  loading: boolean
  isUserAdded: boolean // true = added via '+' button, false = auto-loaded or scaffold
}

function EditorSkeleton() {
  return (
    <div className="flex flex-1 items-center justify-center bg-[#1e1e1e]">
      <div className="h-6 w-6 animate-spin rounded-full border-2 border-slate-500 border-t-slate-300" />
    </div>
  )
}

function isReadOnlyPath(path: string): boolean {
  return path.startsWith('generated/') || path.startsWith('.git/') || path === '.lhp_state.json'
}

const CATEGORY_ICON: Record<string, string> = {
  yaml: '📄',
  sql: '🔍',
  python: '🐍',
  schema: '📐',
  expectations: '✓',
}

function generateScaffoldYaml(pipeline: string, name: string): string {
  return `pipeline: ${pipeline}\nflowgroup: ${name}\ndescription: ""\n\nactions: []\n`
}

interface AddFileOption {
  label: string
  category: string
  defaultPath: (name: string) => string
}

const ADD_FILE_OPTIONS: AddFileOption[] = [
  { label: 'SQL file', category: 'sql', defaultPath: (n) => `sql/${n}.sql` },
  { label: 'Python module', category: 'python', defaultPath: (n) => `py_functions/${n}.py` },
  { label: 'Expectations', category: 'expectations', defaultPath: (n) => `expectations/${n}_quality.json` },
  { label: 'Schema', category: 'schema', defaultPath: (n) => `schemas/${n}_schema.json` },
]

export function FlowgroupEditorModal() {
  const flowgroupEditor = useUIStore((s) => s.flowgroupEditor)
  const closeFlowgroupEditor = useUIStore((s) => s.closeFlowgroupEditor)
  const selectedEnv = useUIStore((s) => s.selectedEnv)

  const isCreateMode = flowgroupEditor?.mode === 'create'

  const { data: relatedData, isLoading: relatedLoading } = useFlowgroupRelatedFiles(
    // Skip fetching related files in create mode — there's no existing flowgroup
    isCreateMode ? null : (flowgroupEditor?.name ?? null),
    selectedEnv,
  )

  const [tabs, setTabs] = useState<TabState[]>([])
  const [activeTabIndex, setActiveTabIndex] = useState(0)
  const [addFileOpen, setAddFileOpen] = useState(false)
  const [addFilePath, setAddFilePath] = useState('')
  const [addFileCategory, setAddFileCategory] = useState('')
  const [isSavingAll, setIsSavingAll] = useState(false)
  const editorRef = useRef<MonacoEditorHandle>(null)
  const queryClient = useQueryClient()
  const initRef = useRef<string | null>(null)

  // Build tabs when relatedData arrives (edit mode only)
  useEffect(() => {
    if (isCreateMode) return
    if (!relatedData || !flowgroupEditor) return

    // Avoid re-initializing if we already built tabs for this flowgroup
    const key = `${relatedData.flowgroup}:${relatedData.environment}`
    if (initRef.current === key) return
    initRef.current = key

    const allFiles: Array<RelatedFileInfo & { isSource: boolean }> = [
      { ...relatedData.source_file, isSource: true },
      ...relatedData.related_files.map((f) => ({ ...f, isSource: false })),
    ]

    const newTabs: TabState[] = allFiles.map((f) => ({
      path: f.path,
      category: f.isSource ? 'yaml' : f.category,
      content: '',
      originalContent: '',
      isDirty: false,
      isSaving: false,
      exists: f.exists,
      isNew: !f.exists,
      loading: f.exists,
      isUserAdded: false,
    }))

    setTabs(newTabs)
    setActiveTabIndex(0)

    // Fetch content for files that exist
    allFiles.forEach((f, i) => {
      if (f.exists) {
        fetchFilePath(f.path)
          .then((res) => {
            const content = res.content ?? ''
            setTabs((prev) =>
              prev.map((tab, idx) =>
                idx === i
                  ? { ...tab, content, originalContent: content, loading: false }
                  : tab,
              ),
            )
          })
          .catch(() => {
            setTabs((prev) =>
              prev.map((tab, idx) =>
                idx === i ? { ...tab, loading: false } : tab,
              ),
            )
          })
      }
    })
  }, [relatedData, flowgroupEditor, isCreateMode])

  // Initialize tabs for create mode
  useEffect(() => {
    if (!isCreateMode || !flowgroupEditor) return

    const key = `create:${flowgroupEditor.name}:${flowgroupEditor.filePath}`
    if (initRef.current === key) return
    initRef.current = key

    const scaffold = generateScaffoldYaml(flowgroupEditor.pipeline, flowgroupEditor.name)
    setTabs([
      {
        path: flowgroupEditor.filePath!,
        category: 'yaml',
        content: scaffold,
        originalContent: '',
        isDirty: true,
        isSaving: false,
        exists: false,
        isNew: true,
        loading: false,
        isUserAdded: false,
      },
    ])
    setActiveTabIndex(0)
  }, [isCreateMode, flowgroupEditor])

  // Reset init tracking when modal closes
  useEffect(() => {
    if (!flowgroupEditor) {
      initRef.current = null
      setTabs([])
      setActiveTabIndex(0)
      setAddFileOpen(false)
      setAddFilePath('')
      setAddFileCategory('')
      setIsSavingAll(false)
    }
  }, [flowgroupEditor])

  // Capture current editor content before switching tabs
  const captureCurrentContent = useCallback(() => {
    if (editorRef.current && tabs.length > 0) {
      const value = editorRef.current.getValue()
      setTabs((prev) =>
        prev.map((tab, idx) =>
          idx === activeTabIndex ? { ...tab, content: value } : tab,
        ),
      )
    }
  }, [activeTabIndex, tabs.length])

  const handleTabSwitch = useCallback(
    (newIndex: number) => {
      if (newIndex === activeTabIndex) return
      captureCurrentContent()
      setActiveTabIndex(newIndex)
    },
    [activeTabIndex, captureCurrentContent],
  )

  const handleDirtyChange = useCallback(
    (dirty: boolean) => {
      setTabs((prev) =>
        prev.map((tab, idx) =>
          idx === activeTabIndex ? { ...tab, isDirty: dirty } : tab,
        ),
      )
    },
    [activeTabIndex],
  )

  // Single-tab save (edit mode)
  const handleSave = useCallback(async () => {
    const tab = tabs[activeTabIndex]
    if (!tab || tab.isSaving) return
    if (isReadOnlyPath(tab.path)) {
      toast.error('This file is read-only')
      return
    }

    const content = editorRef.current?.getValue() ?? ''

    setTabs((prev) =>
      prev.map((t, i) => (i === activeTabIndex ? { ...t, isSaving: true } : t)),
    )

    try {
      await writeFile(tab.path, content)
      setTabs((prev) =>
        prev.map((t, i) =>
          i === activeTabIndex
            ? {
                ...t,
                content,
                originalContent: content,
                isDirty: false,
                isSaving: false,
                exists: true,
                isNew: false,
              }
            : t,
        ),
      )
      const filename = tab.path.split('/').pop() ?? tab.path
      toast.success(`Saved ${filename}`)
      queryClient.invalidateQueries({ queryKey: ['files'] })
      queryClient.invalidateQueries({ queryKey: ['git-status'] })
    } catch (err) {
      const message = err instanceof ApiError ? err.message : 'Failed to save file'
      toast.error(message)
      setTabs((prev) =>
        prev.map((t, i) => (i === activeTabIndex ? { ...t, isSaving: false } : t)),
      )
    }
  }, [tabs, activeTabIndex, queryClient])

  // Create-mode save: write all tabs
  const handleCreateSave = useCallback(async () => {
    if (isSavingAll) return

    // Capture the active editor's content first
    captureCurrentContent()

    setIsSavingAll(true)
    // Mark all tabs as saving
    setTabs((prev) => prev.map((t) => ({ ...t, isSaving: true })))

    let allSucceeded = true
    const currentTabs = [...tabs]

    // Update the active tab content from editor
    if (editorRef.current) {
      currentTabs[activeTabIndex] = {
        ...currentTabs[activeTabIndex],
        content: editorRef.current.getValue(),
      }
    }

    // Save YAML tab first (index 0)
    for (let i = 0; i < currentTabs.length; i++) {
      const tab = currentTabs[i]
      try {
        await writeFile(tab.path, tab.content)
        setTabs((prev) =>
          prev.map((t, idx) =>
            idx === i
              ? {
                  ...t,
                  originalContent: tab.content,
                  isDirty: false,
                  isSaving: false,
                  exists: true,
                  isNew: false,
                }
              : t,
          ),
        )
      } catch (err) {
        allSucceeded = false
        const message = err instanceof ApiError ? err.message : 'Failed to save file'
        const filename = tab.path.split('/').pop() ?? tab.path
        toast.error(`Failed to save ${filename}: ${message}`)
        setTabs((prev) =>
          prev.map((t, idx) => (idx === i ? { ...t, isSaving: false } : t)),
        )
      }
    }

    setIsSavingAll(false)

    if (allSucceeded) {
      toast.success(`Created ${flowgroupEditor?.name}`)
      queryClient.invalidateQueries({ queryKey: ['flowgroups'] })
      queryClient.invalidateQueries({ queryKey: ['pipelines'] })
      queryClient.invalidateQueries({ queryKey: ['dep-graph'] })
      queryClient.invalidateQueries({ queryKey: ['execution-order'] })
      queryClient.invalidateQueries({ queryKey: ['circular-deps'] })
      queryClient.invalidateQueries({ queryKey: ['files'] })
      queryClient.invalidateQueries({ queryKey: ['git-status'] })
      closeFlowgroupEditor()
    } else {
      toast.warning('Some files failed to save. Fix errors and try again.')
    }
  }, [tabs, activeTabIndex, isSavingAll, captureCurrentContent, flowgroupEditor, queryClient, closeFlowgroupEditor])

  const handleClose = useCallback(() => {
    const dirtyCount = tabs.filter((t) => t.isDirty).length
    if (dirtyCount > 0) {
      if (
        !window.confirm(
          `You have unsaved changes in ${dirtyCount} file(s). Discard and close?`,
        )
      )
        return
    }
    closeFlowgroupEditor()
  }, [tabs, closeFlowgroupEditor])

  // Add file tab
  const handleAddFileSelect = useCallback(
    (option: AddFileOption) => {
      if (!flowgroupEditor) return
      setAddFileCategory(option.category)
      setAddFilePath(option.defaultPath(flowgroupEditor.name))
      setAddFileOpen(false)
    },
    [flowgroupEditor],
  )

  const handleAddFileConfirm = useCallback(() => {
    if (!addFilePath || !addFileCategory) return
    // Check for duplicate path
    if (tabs.some((t) => t.path === addFilePath)) {
      toast.error('A tab with this path already exists')
      return
    }

    captureCurrentContent()
    const newTab: TabState = {
      path: addFilePath,
      category: addFileCategory,
      content: '',
      originalContent: '',
      isDirty: true,
      isSaving: false,
      exists: false,
      isNew: true,
      loading: false,
      isUserAdded: true,
    }
    setTabs((prev) => [...prev, newTab])
    setActiveTabIndex(tabs.length)
    setAddFilePath('')
    setAddFileCategory('')
  }, [addFilePath, addFileCategory, tabs, captureCurrentContent])

  const handleAddFileCancel = useCallback(() => {
    setAddFilePath('')
    setAddFileCategory('')
  }, [])

  // Remove a user-added file tab
  const handleRemoveTab = useCallback(
    (index: number) => {
      if (index === 0) return // Don't remove the YAML tab
      setTabs((prev) => prev.filter((_, i) => i !== index))
      if (activeTabIndex >= index && activeTabIndex > 0) {
        setActiveTabIndex(activeTabIndex - 1)
      }
    },
    [activeTabIndex],
  )

  // Escape handler (capture phase — intercepts before lower modals)
  useEffect(() => {
    if (!flowgroupEditor) return

    const handleEscape = (e: KeyboardEvent) => {
      if (e.key !== 'Escape') return
      const findWidget = document.querySelector('.monaco-editor .find-widget.visible')
      if (findWidget) return
      e.stopPropagation()
      handleClose()
    }

    document.addEventListener('keydown', handleEscape, { capture: true })
    return () => document.removeEventListener('keydown', handleEscape, { capture: true })
  }, [flowgroupEditor, handleClose])

  if (!flowgroupEditor) return null

  const activeTab = tabs[activeTabIndex]
  const isReadOnly = activeTab ? isReadOnlyPath(activeTab.path) : false
  const showLoading = !isCreateMode && relatedLoading

  return (
    <div
      className="fixed inset-0 z-[55] flex items-center justify-center bg-black/40"
      onClick={(e) => {
        if (e.target === e.currentTarget) handleClose()
      }}
      onKeyDown={(e) => e.stopPropagation()}
    >
      <div className="flex h-[85vh] w-full max-w-5xl flex-col overflow-hidden rounded-lg border border-slate-200 shadow-xl">
        {/* Header */}
        <div className="flex items-center justify-between border-b border-slate-200 bg-white px-5 py-3">
          <div className="min-w-0 flex-1">
            <div className="flex items-center gap-2">
              <span className="text-[10px] font-semibold uppercase tracking-wider text-slate-400">
                {isCreateMode ? 'New Flowgroup' : 'Flowgroup'}
              </span>
              <h2 className="truncate text-sm font-semibold text-slate-800">
                {flowgroupEditor.name}
              </h2>
            </div>
          </div>
          <div className="ml-3 flex shrink-0 items-center gap-2">
            {isCreateMode ? (
              <button
                onClick={handleCreateSave}
                disabled={isSavingAll}
                className="rounded bg-blue-600 px-3 py-1.5 text-[11px] font-medium text-white hover:bg-blue-700 disabled:opacity-40"
              >
                {isSavingAll ? 'Creating...' : 'Create All'}
              </button>
            ) : (
              activeTab && !isReadOnly && (
                <button
                  onClick={handleSave}
                  disabled={!activeTab.isDirty || activeTab.isSaving}
                  className="rounded bg-blue-600 px-3 py-1.5 text-[11px] font-medium text-white hover:bg-blue-700 disabled:opacity-40"
                >
                  {activeTab.isSaving ? 'Saving...' : 'Save'}
                </button>
              )
            )}
            <button
              onClick={handleClose}
              className="rounded p-1.5 text-slate-400 hover:bg-slate-100 hover:text-slate-600"
            >
              <svg className="h-5 w-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path
                  strokeLinecap="round"
                  strokeLinejoin="round"
                  strokeWidth={2}
                  d="M6 18L18 6M6 6l12 12"
                />
              </svg>
            </button>
          </div>
        </div>

        {/* Tab bar wrapper — relative so the dropdown can escape the overflow container */}
        <div className="relative border-b border-slate-200 bg-slate-50">
          <div className="flex overflow-x-auto px-2">
            {showLoading ? (
              <div className="flex items-center gap-2 px-3 py-2 text-xs text-slate-400">
                <div className="h-3 w-3 animate-spin rounded-full border border-slate-300 border-t-slate-500" />
                Loading files...
              </div>
            ) : (
              <>
                {tabs.map((tab, i) => {
                  const filename = tab.path.split('/').pop() ?? tab.path
                  const isActive = i === activeTabIndex
                  return (
                    <button
                      key={tab.path}
                      onClick={() => handleTabSwitch(i)}
                      title={tab.path}
                      className={`group relative flex shrink-0 items-center gap-1.5 border-b-2 px-3 py-2 text-[11px] font-medium transition-colors ${
                        isActive
                          ? 'border-blue-500 text-blue-700'
                          : 'border-transparent text-slate-500 hover:text-slate-700'
                      } ${!tab.exists ? 'italic opacity-60' : ''}`}
                    >
                      {!tab.exists && (
                        <span className="text-[10px]" title="File doesn't exist — edit and save to create">
                          +
                        </span>
                      )}
                      <span>{filename}</span>
                      {tab.isDirty && (
                        <span className="ml-0.5 inline-block h-1.5 w-1.5 rounded-full bg-amber-500" />
                      )}
                      {/* Remove button for user-added tabs */}
                      {tab.isUserAdded && (
                        <span
                          onClick={(e) => {
                            e.stopPropagation()
                            handleRemoveTab(i)
                          }}
                          className="ml-1 hidden cursor-pointer text-slate-400 hover:text-red-500 group-hover:inline"
                          title="Remove tab"
                        >
                          ×
                        </span>
                      )}
                    </button>
                  )
                })}

                {/* Add file button */}
                <button
                  onClick={() => setAddFileOpen(!addFileOpen)}
                  title="Add a file tab"
                  className="flex h-6 w-6 shrink-0 items-center justify-center self-center rounded text-slate-400 hover:bg-slate-200 hover:text-slate-600"
                >
                  <svg className="h-3.5 w-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 4v16m8-8H4" />
                  </svg>
                </button>
              </>
            )}
          </div>

          {/* Dropdown rendered outside overflow container so it isn't clipped */}
          {addFileOpen && (
            <div className="absolute right-2 top-full z-20 mt-1 w-44 rounded border border-slate-200 bg-white py-1 shadow-lg">
              {ADD_FILE_OPTIONS.map((opt) => (
                <button
                  key={opt.category}
                  onClick={() => handleAddFileSelect(opt)}
                  className="block w-full px-3 py-1.5 text-left text-xs text-slate-700 hover:bg-slate-100"
                >
                  {CATEGORY_ICON[opt.category] ?? ''} {opt.label}
                </button>
              ))}
            </div>
          )}
        </div>

        {/* Add file path input (shown after selecting a file type) */}
        {addFileCategory && (
          <div className="flex items-center gap-2 border-b border-slate-200 bg-slate-100 px-4 py-2">
            <span className="text-xs text-slate-500">Path:</span>
            <input
              type="text"
              value={addFilePath}
              onChange={(e) => setAddFilePath(e.target.value)}
              className="flex-1 rounded border border-slate-300 bg-white px-2 py-1 font-mono text-xs text-slate-800 focus:border-blue-400 focus:outline-none focus:ring-1 focus:ring-blue-400"
              autoFocus
              onKeyDown={(e) => {
                if (e.key === 'Enter') handleAddFileConfirm()
                if (e.key === 'Escape') handleAddFileCancel()
              }}
            />
            <button
              onClick={handleAddFileConfirm}
              className="rounded bg-blue-600 px-2 py-1 text-[10px] font-medium text-white hover:bg-blue-700"
            >
              Add
            </button>
            <button
              onClick={handleAddFileCancel}
              className="rounded px-2 py-1 text-[10px] font-medium text-slate-500 hover:bg-slate-200"
            >
              Cancel
            </button>
          </div>
        )}

        {/* Editor body */}
        <div className="flex min-h-0 flex-1 flex-col bg-[#1e1e1e]">
          {/* Non-existent file banner */}
          {activeTab && !activeTab.exists && !isCreateMode && (
            <div className="border-b border-slate-700 bg-slate-800 px-4 py-2 text-xs text-slate-400">
              This file doesn't exist yet. Save to create it.
            </div>
          )}

          {/* Monaco editor */}
          {activeTab && !activeTab.loading ? (
            <div className="min-h-0 flex-1">
              <Suspense fallback={<EditorSkeleton />}>
                <MonacoEditorWrapper
                  key={activeTab.path}
                  ref={editorRef}
                  path={activeTab.path}
                  content={activeTab.content}
                  readOnly={isReadOnly}
                  onDirtyChange={handleDirtyChange}
                  onSave={isCreateMode ? handleCreateSave : handleSave}
                />
              </Suspense>
            </div>
          ) : (
            <EditorSkeleton />
          )}
        </div>
      </div>
    </div>
  )
}
