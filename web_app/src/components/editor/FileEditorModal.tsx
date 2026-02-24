import { lazy, Suspense, useEffect, useRef, useCallback } from 'react'
import { useQueryClient } from '@tanstack/react-query'
import { toast } from 'sonner'
import { useUIStore } from '../../store/uiStore'
import { writeFile } from '../../api/files'
import { ApiError } from '../../api/client'
import type { MonacoEditorHandle } from './MonacoEditorWrapper'

const MonacoEditorWrapper = lazy(() => import('./MonacoEditorWrapper'))

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

export function FileEditorModal() {
  const openFile = useUIStore((s) => s.openFile)
  const closeFile = useUIStore((s) => s.closeFile)
  const setFileDirty = useUIStore((s) => s.setFileDirty)
  const setFileSaving = useUIStore((s) => s.setFileSaving)
  const updateFileContent = useUIStore((s) => s.updateFileContent)

  const editorRef = useRef<MonacoEditorHandle>(null)
  const queryClient = useQueryClient()

  const isReadOnlyFile = openFile ? isReadOnlyPath(openFile.path) : false

  const handleSave = useCallback(async () => {
    if (!openFile || openFile.isSaving) return
    if (isReadOnlyFile) {
      toast.error('This file is read-only')
      return
    }

    const content = editorRef.current?.getValue() ?? ''
    setFileSaving(true)
    try {
      await writeFile(openFile.path, content)
      updateFileContent(content)
      const filename = openFile.path.split('/').pop() ?? openFile.path
      toast.success(`Saved ${filename}`)
      queryClient.invalidateQueries({ queryKey: ['files'] })
      queryClient.invalidateQueries({ queryKey: ['git-status'] })
    } catch (err) {
      const message = err instanceof ApiError ? err.message : 'Failed to save file'
      toast.error(message)
    } finally {
      setFileSaving(false)
    }
  }, [openFile, isReadOnlyFile, setFileSaving, updateFileContent, queryClient])

  const handleClose = useCallback(() => {
    if (openFile?.isDirty) {
      if (!window.confirm('You have unsaved changes. Discard and close?')) return
    }
    closeFile()
  }, [openFile?.isDirty, closeFile])

  // Capture-phase Escape handler — intercepts before lower modals
  useEffect(() => {
    if (!openFile) return

    const handleEscape = (e: KeyboardEvent) => {
      if (e.key !== 'Escape') return

      // Let Monaco close its own find widget first
      const findWidget = document.querySelector('.monaco-editor .find-widget.visible')
      if (findWidget) return

      e.stopPropagation()
      handleClose()
    }

    document.addEventListener('keydown', handleEscape, { capture: true })
    return () => document.removeEventListener('keydown', handleEscape, { capture: true })
  }, [openFile, handleClose])

  if (!openFile) return null

  const filename = openFile.path.split('/').pop() ?? openFile.path

  return (
    <div
      className="fixed inset-0 z-[55] flex items-center justify-center bg-black/40"
      onClick={(e) => {
        if (e.target === e.currentTarget) handleClose()
      }}
      onKeyDown={(e) => e.stopPropagation()}
    >
      <div className="flex h-[85vh] w-full max-w-5xl flex-col overflow-hidden rounded-lg border border-slate-200 shadow-xl">
        {/* Light header */}
        <div className="flex items-center justify-between border-b border-slate-200 bg-white px-5 py-3">
          <div className="min-w-0 flex-1">
            <div className="flex items-center gap-2">
              <h2 className="truncate text-sm font-semibold text-slate-800">{filename}</h2>
              {isReadOnlyFile && (
                <span className="shrink-0 rounded bg-slate-100 px-1.5 py-0.5 text-[10px] font-medium text-slate-500">
                  Read Only
                </span>
              )}
              {openFile.isDirty && !isReadOnlyFile && (
                <span className="shrink-0 rounded bg-amber-50 px-1.5 py-0.5 text-[10px] font-medium text-amber-600">
                  Modified
                </span>
              )}
            </div>
            <p className="truncate font-mono text-[11px] text-slate-400">{openFile.path}</p>
          </div>
          <div className="ml-3 flex shrink-0 items-center gap-2">
            {!isReadOnlyFile && (
              <button
                onClick={handleSave}
                disabled={!openFile.isDirty || openFile.isSaving}
                className="rounded bg-blue-600 px-3 py-1.5 text-[11px] font-medium text-white hover:bg-blue-700 disabled:opacity-40"
              >
                {openFile.isSaving ? 'Saving...' : 'Save'}
              </button>
            )}
            <button
              onClick={handleClose}
              className="rounded p-1.5 text-slate-400 hover:bg-slate-100 hover:text-slate-600"
            >
              <svg className="h-5 w-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
              </svg>
            </button>
          </div>
        </div>

        {/* Dark editor body */}
        <div className="flex min-h-0 flex-1 bg-[#1e1e1e]">
          <Suspense fallback={<EditorSkeleton />}>
            <MonacoEditorWrapper
              ref={editorRef}
              path={openFile.path}
              content={openFile.content}
              readOnly={isReadOnlyFile}
              onDirtyChange={setFileDirty}
              onSave={handleSave}
            />
          </Suspense>
        </div>
      </div>
    </div>
  )
}
