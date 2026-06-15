import { useState, useCallback } from 'react'
import { useQueryClient } from '@tanstack/react-query'
import { toast } from 'sonner'
import { useFileList } from '../../hooks/useFiles'
import { useUIStore } from '../../store/uiStore'
import { fetchFileContent, writeFile, deleteFile } from '../../api/files'
import { ApiError } from '../../api/client'
import { SkeletonLoader } from '../common/SkeletonLoader'
import { FileTreeItem } from './FileTreeItem'

function errorMessage(err: unknown, fallback: string): string {
  if (err instanceof ApiError) {
    // The file API returns 403 for write-protected paths
    // (.git/, generated/, .lhp/logs/, .lhp/dependencies/, .lhp_state.json).
    if (err.status === 403) {
      return 'This path is write-protected and cannot be modified.'
    }
    return err.message
  }
  return fallback
}

export function FileBrowser() {
  const { data, isLoading } = useFileList()
  const openFilePath = useUIStore((s) => s.openFilePath)
  const queryClient = useQueryClient()
  const [expandedPaths, setExpandedPaths] = useState<Set<string>>(new Set())
  const [creating, setCreating] = useState(false)
  const [newPath, setNewPath] = useState('')
  const [submitting, setSubmitting] = useState(false)

  const handleToggle = useCallback((path: string) => {
    setExpandedPaths((prev) => {
      const next = new Set(prev)
      if (next.has(path)) {
        next.delete(path)
      } else {
        next.add(path)
      }
      return next
    })
  }, [])

  const handleFileClick = useCallback(
    async (path: string) => {
      try {
        const content = await fetchFileContent(path)
        openFilePath(path, content)
      } catch {
        // 404 / 403 / network error — silently skip
      }
    },
    [openFilePath],
  )

  const startCreate = useCallback(() => {
    setNewPath('')
    setCreating(true)
  }, [])

  const cancelCreate = useCallback(() => {
    setCreating(false)
    setNewPath('')
  }, [])

  const submitCreate = useCallback(async () => {
    const path = newPath.trim().replace(/^\/+/, '')
    if (!path || submitting) return
    setSubmitting(true)
    try {
      await writeFile(path, '')
      queryClient.invalidateQueries({ queryKey: ['files'] })
      setCreating(false)
      setNewPath('')
      const filename = path.split('/').pop() ?? path
      toast.success(`Created ${filename}`)
      openFilePath(path, '')
    } catch (err) {
      toast.error(errorMessage(err, 'Failed to create file'))
    } finally {
      setSubmitting(false)
    }
  }, [newPath, submitting, queryClient, openFilePath])

  const handleDelete = useCallback(
    async (path: string) => {
      const filename = path.split('/').pop() ?? path
      if (!window.confirm(`Delete ${filename}? This cannot be undone.`)) return
      try {
        await deleteFile(path)
        queryClient.invalidateQueries({ queryKey: ['files'] })
        toast.success(`Deleted ${filename}`)
      } catch (err) {
        toast.error(errorMessage(err, 'Failed to delete file'))
      }
    },
    [queryClient],
  )

  if (isLoading) return <SkeletonLoader lines={6} />

  return (
    <div className="space-y-0.5 px-1 py-2">
      <div className="flex items-center justify-between px-2 pb-1">
        <span className="text-[10px] font-semibold uppercase tracking-wider text-slate-400">
          File Browser
        </span>
        <button
          type="button"
          onClick={startCreate}
          title="New file"
          className="rounded p-0.5 text-slate-400 hover:bg-slate-100 hover:text-slate-600"
        >
          <svg className="h-3.5 w-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 4v16m8-8H4" />
          </svg>
        </button>
      </div>

      {creating && (
        <div className="px-2 pb-1">
          <input
            autoFocus
            type="text"
            value={newPath}
            placeholder="path/to/new_file.yaml"
            disabled={submitting}
            onChange={(e) => setNewPath(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === 'Enter') {
                e.preventDefault()
                void submitCreate()
              } else if (e.key === 'Escape') {
                e.preventDefault()
                cancelCreate()
              }
            }}
            onBlur={cancelCreate}
            className="w-full rounded border border-slate-300 px-2 py-1 font-mono text-[11px] text-slate-700 focus:border-blue-400 focus:outline-none disabled:opacity-50"
          />
        </div>
      )}

      {data?.children?.map((node) => (
        <FileTreeItem
          key={node.path}
          node={node}
          depth={1}
          expandedPaths={expandedPaths}
          onToggle={handleToggle}
          onClick={handleFileClick}
          onDelete={handleDelete}
        />
      ))}
    </div>
  )
}
