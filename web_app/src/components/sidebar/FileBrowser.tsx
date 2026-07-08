import { useState, useCallback } from 'react'
import { useQueryClient } from '@tanstack/react-query'
import { toast } from 'sonner'
import { FilePlus2 } from 'lucide-react'
import { useFileList } from '../../hooks/useFiles'
import { useWorkspaceStore } from '../../store/workspaceStore'
import { fetchFileContentWithMeta, writeFile, deleteFile } from '../../api/files'
import { errorMessage } from '../../lib/errors'
import { SkeletonLoader } from '../common/SkeletonLoader'
import { Button } from '../ui/button'
import { Input } from '../ui/input'
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from '../ui/alert-dialog'
import { FileTreeItem } from './FileTreeItem'

export function FileBrowser() {
  const { data, isLoading } = useFileList()
  const openBuffer = useWorkspaceStore((s) => s.openBuffer)
  const setActiveBuffer = useWorkspaceStore((s) => s.setActive)
  const activeFilePath = useWorkspaceStore((s) => s.activePath)
  const queryClient = useQueryClient()
  const [expandedPaths, setExpandedPaths] = useState<Set<string>>(new Set())
  const [creating, setCreating] = useState(false)
  const [newPath, setNewPath] = useState('')
  const [submitting, setSubmitting] = useState(false)
  const [pendingDelete, setPendingDelete] = useState<string | null>(null)

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
      // Already open in the workspace → just focus it (never clobber edits).
      if (useWorkspaceStore.getState().buffers.some((b) => b.path === path)) {
        setActiveBuffer(path)
        return
      }
      try {
        const { content, etag } = await fetchFileContentWithMeta(path)
        openBuffer(path, { content, etag, exists: true })
      } catch (err) {
        toast.error(errorMessage(err, 'Failed to open file'))
      }
    },
    [openBuffer, setActiveBuffer],
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
      const res = await writeFile(path, '')
      queryClient.invalidateQueries({ queryKey: ['files'] })
      setCreating(false)
      setNewPath('')
      const filename = path.split('/').pop() ?? path
      toast.success(`Created ${filename}`)
      openBuffer(path, { content: '', etag: res.etag ?? null, exists: true })
    } catch (err) {
      toast.error(errorMessage(err, 'Failed to create file'))
    } finally {
      setSubmitting(false)
    }
  }, [newPath, submitting, queryClient, openBuffer])

  const confirmDelete = useCallback(
    async (path: string) => {
      const filename = path.split('/').pop() ?? path
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

  const pendingDeleteName = pendingDelete?.split('/').pop() ?? pendingDelete

  return (
    <div className="space-y-0.5 px-1 py-2">
      <div className="flex items-center justify-between px-2 pb-1">
        <span className="text-2xs font-semibold uppercase tracking-[0.05em] text-muted-foreground">
          File Browser
        </span>
        <Button
          variant="ghost"
          size="icon-xs"
          onClick={startCreate}
          aria-label="New file"
          title="New file"
          className="text-muted-foreground"
        >
          <FilePlus2 />
        </Button>
      </div>

      {creating && (
        <div className="px-2 pb-1">
          <Input
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
            className="h-7 px-2 font-mono text-xs md:text-xs"
          />
        </div>
      )}

      {/* Junk filter (dotfiles like .DS_Store) — FileTreeItem applies the
          same predicate to nested children. */}
      {data?.children?.filter((node) => !node.name.startsWith('.')).map((node) => (
        <FileTreeItem
          key={node.path}
          node={node}
          depth={1}
          expandedPaths={expandedPaths}
          activePath={activeFilePath}
          onToggle={handleToggle}
          onClick={handleFileClick}
          onDelete={setPendingDelete}
        />
      ))}

      <AlertDialog
        open={pendingDelete !== null}
        onOpenChange={(open) => {
          if (!open) setPendingDelete(null)
        }}
      >
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Delete {pendingDeleteName}?</AlertDialogTitle>
            <AlertDialogDescription>
              This permanently deletes the file. This cannot be undone.
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>Cancel</AlertDialogCancel>
            <AlertDialogAction
              variant="destructive"
              onClick={() => {
                if (pendingDelete) void confirmDelete(pendingDelete)
                setPendingDelete(null)
              }}
            >
              Delete
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </div>
  )
}
