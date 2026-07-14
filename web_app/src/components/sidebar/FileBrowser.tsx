import { useState, useCallback, useMemo } from 'react'
import { useQueryClient } from '@tanstack/react-query'
import { toast } from 'sonner'
import { Boxes, FilePlus2 } from 'lucide-react'
import { useFileList } from '../../hooks/useFiles'
import { useFlowgroups } from '../../hooks/useFlowgroups'
import { useWorkspaceStore, entityTemplateTabId } from '../../store/workspaceStore'
import { useUIStore } from '../../store/uiStore'
import { useSandboxScope } from '../sandbox/useSandboxScope'
import {
  buildSourceFileToPipeline,
  filterFileTreeForScope,
} from '../sandbox/scopeFilter'
import { fetchFileContentWithMeta, writeFile, deleteFile } from '../../api/files'
import { errorMessage } from '../../lib/errors'
import { parseFlowgroupFile, selectTemplate } from '../../lib/flowgroup-doc'
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
  const { data: flowgroups } = useFlowgroups()
  const scope = useSandboxScope()
  const openBuffer = useWorkspaceStore((s) => s.openBuffer)
  const setActiveBuffer = useWorkspaceStore((s) => s.setActive)
  const openEntityTab = useWorkspaceStore((s) => s.openEntityTab)
  const activeFilePath = useWorkspaceStore((s) => s.activePath)
  const openCreateFlowgroupDialog = useUIStore((s) => s.openCreateFlowgroupDialog)
  const queryClient = useQueryClient()
  const [expandedPaths, setExpandedPaths] = useState<Set<string>>(new Set())
  const [creating, setCreating] = useState(false)
  const [newPath, setNewPath] = useState('')
  const [submitting, setSubmitting] = useState(false)
  const [pendingDelete, setPendingDelete] = useState<string | null>(null)

  // Sandbox mode hides pipelines/ files whose pipeline is out of scope. Shared
  // config (lhp.yaml, substitutions/, presets/, …) has no flowgroup mapping,
  // so it always stays visible; scope === null leaves the tree untouched.
  const sourceMap = useMemo(
    () => buildSourceFileToPipeline(flowgroups?.flowgroups ?? []),
    [flowgroups],
  )
  const scopedTree = useMemo(
    () => (data ? filterFileTreeForScope(data, sourceMap, scope) : undefined),
    [data, sourceMap, scope],
  )

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
      const ws = useWorkspaceStore.getState()
      // Already open as a text buffer → just focus it (never clobber edits).
      if (ws.buffers.some((b) => b.path === path)) {
        setActiveBuffer(path)
        return
      }
      // Already open as a template entity tab → focus that.
      const templateTabId = entityTemplateTabId(path)
      if (ws.tabs.some((t) => t.kind === 'entity' && t.docKind === 'template' && t.filePath === path)) {
        setActiveBuffer(templateTabId)
        return
      }
      try {
        const { content, etag } = await fetchFileContentWithMeta(path)
        // A file under templates/ that parses as a template opens as a template
        // entity tab (Graph / Code, default Graph); anything else — including a
        // non-template YAML that happens to live there — as text.
        if (isTemplatePath(path)) {
          const file = parseFlowgroupFile(content)
          const template = file.errors.length === 0 ? selectTemplate(file) : undefined
          if (template) {
            openEntityTab('', template.info.name || filenameStem(path), path, {
              docKind: 'template',
            })
            return
          }
        }
        openBuffer(path, { content, etag, exists: true })
      } catch (err) {
        toast.error(errorMessage(err, 'Failed to open file'))
      }
    },
    [openBuffer, setActiveBuffer, openEntityTab],
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
        <div className="flex items-center gap-0.5">
          <Button
            variant="ghost"
            size="icon-xs"
            onClick={() => openCreateFlowgroupDialog()}
            aria-label="New flowgroup"
            title="New flowgroup"
            className="text-muted-foreground"
          >
            <Boxes />
          </Button>
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
      {scopedTree?.children?.filter((node) => !node.name.startsWith('.')).map((node) => (
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

/** A YAML file under templates/ — the designer's template-authoring surface. */
function isTemplatePath(path: string): boolean {
  return /^templates\//.test(path) && /\.ya?ml$/i.test(path)
}

/** Filename without its extension, as a display fallback when a template
 * declares no `name`. */
function filenameStem(path: string): string {
  const name = path.split('/').pop() ?? path
  return name.replace(/\.[^.]+$/, '')
}
