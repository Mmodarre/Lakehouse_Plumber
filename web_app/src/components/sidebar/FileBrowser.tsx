import { useState, useCallback, useMemo, useRef } from 'react'
import { useQueryClient } from '@tanstack/react-query'
import { toast } from 'sonner'
import { Boxes, FilePlus2, LocateFixed } from 'lucide-react'
import { useFileList } from '../../hooks/useFiles'
import { useFlowgroups } from '../../hooks/useFlowgroups'
import { useWorkspaceStore, tabBufferPath, workspaceTabId, isReadOnlyPath } from '../../store/workspaceStore'
import { useLayoutStore } from '../../store/layoutStore'
import { openWorkspaceFile, configurationKindForPath } from '../../workspace/openWorkspaceFile'
import { captureWorkspaceEditors } from '../../workspace/editorCommands'
import { ApiError } from '../../api/client'
import { useUIStore } from '../../store/uiStore'
import { useSandboxScope } from '../sandbox/useSandboxScope'
import {
  buildSourceFileToPipeline,
  filterFileTreeForScope,
} from '../sandbox/scopeFilter'
import { fetchFileContentWithMeta, writeFile, deleteFile, IF_MATCH_CREATE_ONLY } from '../../api/files'
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
  const { data, isLoading, isError, error, refetch } = useFileList()
  const { data: flowgroups } = useFlowgroups()
  const scope = useSandboxScope()
  const openBuffer = useWorkspaceStore((s) => s.openBuffer)
  const openEntityTab = useWorkspaceStore((s) => s.openEntityTab)
  const activeFilePath = useWorkspaceStore((s) => {
    const tab = s.tabs.find((t) => workspaceTabId(t) === s.activePath)
    return tab ? tabBufferPath(tab) : null
  })
  const viewerMode = useLayoutStore((s) => s.viewerMode)
  const treeRef = useRef<HTMLDivElement>(null)
  const fileOpenRequest = useRef(0)
  const openCreateFlowgroupDialog = useUIStore((s) => s.openCreateFlowgroupDialog)
  const queryClient = useQueryClient()
  const [expandedPaths, setExpandedPaths] = useState<Set<string>>(new Set())
  const [creating, setCreating] = useState(false)
  const [newPath, setNewPath] = useState('')
  const [submitting, setSubmitting] = useState(false)
  const [pendingDelete, setPendingDelete] = useState<string | null>(null)
  const [duplicateContent, setDuplicateContent] = useState<string | null>(null)
  const canMutate = useCallback((path: string) => !viewerMode && !isReadOnlyPath(path.endsWith('/') ? path : `${path}/`) && !isReadOnlyPath(path), [viewerMode])

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
      const request = ++fileOpenRequest.current
      const ws = useWorkspaceStore.getState()
      if (!isTemplatePath(path) || ws.buffers.some((b) => b.path === path) || ws.tabs.some((t) => tabBufferPath(t) === path) || configurationKindForPath(path)) {
        try { await openWorkspaceFile(path) } catch (err) { toast.error(errorMessage(err, 'Failed to open file')) }
        return
      }
      try {
        const { content, etag } = await fetchFileContentWithMeta(path)
        const current = useWorkspaceStore.getState()
        if (current.projectRoot !== ws.projectRoot) return
        const activate = request === fileOpenRequest.current && current.activePath === ws.activePath
        // A file under templates/ that parses as a template opens as a template
        // entity tab (Graph / Code, default Graph); anything else — including a
        // non-template YAML that happens to live there — as text.
        if (isTemplatePath(path)) {
          const file = parseFlowgroupFile(content)
          const template = file.errors.length === 0 ? selectTemplate(file) : undefined
          if (template) {
            openEntityTab('', template.info.name || filenameStem(path), path, {
              docKind: 'template', activate,
            })
            return
          }
        }
        openBuffer(path, { content, etag, exists: true, activate })
      } catch (err) {
        toast.error(errorMessage(err, 'Failed to open file'))
      }
    },
    [openBuffer, openEntityTab],
  )

  const startCreate = useCallback((folder = '') => {
    if (!canMutate(folder)) return
    setDuplicateContent(null)
    setNewPath(folder ? `${folder}/` : '')
    setCreating(true)
  }, [canMutate])

  const startDuplicate = useCallback(async (path: string) => {
    if (!canMutate(path)) return
    captureWorkspaceEditors()
    try {
      const buffer = useWorkspaceStore.getState().buffers.find((b) => b.path === path && !b.loading && !b.loadFailed)
      const content = buffer?.content ?? (await fetchFileContentWithMeta(path)).content
      const dot = path.lastIndexOf('.')
      setNewPath(dot > path.lastIndexOf('/') ? `${path.slice(0, dot)}_copy${path.slice(dot)}` : `${path}_copy`)
      setDuplicateContent(content)
      setCreating(true)
    } catch (err) { toast.error(errorMessage(err, 'Failed to duplicate file')) }
  }, [canMutate])

  const openSource = useCallback((path: string) => {
    void openWorkspaceFile(path, { source: true }).catch((err) => toast.error(errorMessage(err, 'Failed to open file')))
  }, [])

  const revealActive = useCallback(() => {
    if (!activeFilePath) return
    const parts = activeFilePath.split('/')
    setExpandedPaths((previous) => {
      const next = new Set(previous)
      for (let i = 1; i < parts.length; i++) next.add(parts.slice(0, i).join('/'))
      return next
    })
    window.requestAnimationFrame(() => {
      const row = Array.from(treeRef.current?.querySelectorAll<HTMLElement>('[data-file-path]') ?? []).find((node) => node.dataset.filePath === activeFilePath)
      row?.scrollIntoView?.({ block: 'nearest' })
      row?.querySelector('button')?.focus()
    })
  }, [activeFilePath])

  const cancelCreate = useCallback(() => {
    setCreating(false)
    setNewPath('')
    setDuplicateContent(null)
  }, [])

  const submitCreate = useCallback(async () => {
    const path = newPath.trim().replace(/^\/+/, '')
    if (!path || submitting || !canMutate(path)) return
    setSubmitting(true)
    try {
      const content = duplicateContent ?? ''
      const res = await writeFile(path, content, IF_MATCH_CREATE_ONLY)
      queryClient.invalidateQueries({ queryKey: ['files'] })
      setCreating(false)
      setNewPath('')
      const filename = path.split('/').pop() ?? path
      toast.success(`Created ${filename}`)
      openBuffer(path, { content, etag: res.etag ?? null, exists: true })
    } catch (err) {
      if (err instanceof ApiError && err.status === 412) toast.error('File already exists', { action: { label: 'Open file', onClick: () => { void handleFileClick(path) } } })
      else toast.error(errorMessage(err, 'Failed to create file'))
    } finally {
      setSubmitting(false)
    }
  }, [newPath, submitting, queryClient, openBuffer, canMutate, duplicateContent, handleFileClick])

  const confirmDelete = useCallback(
    async (path: string) => {
      if (!canMutate(path)) return
      const filename = path.split('/').pop() ?? path
      try {
        await deleteFile(path)
        useWorkspaceStore.setState((state) => ({ buffers: state.buffers.map((b) => b.path === path ? { ...b, exists: false, isNew: true, etag: null, isDirty: true } : b) }))
        queryClient.invalidateQueries({ queryKey: ['files'] })
        toast.success(`Deleted ${filename}`)
      } catch (err) {
        toast.error(errorMessage(err, 'Failed to delete file'))
      }
    },
    [queryClient, canMutate],
  )

  if (isLoading) return <SkeletonLoader lines={6} />
  if (isError) return <div role="alert" className="space-y-2 p-3 text-sm"><p>{errorMessage(error, 'Could not load project files.')}</p><Button size="sm" variant="outline" onClick={() => { void refetch() }}>Retry</Button></div>

  const pendingDeleteName = pendingDelete?.split('/').pop() ?? pendingDelete

  return (
    <div ref={treeRef} className="space-y-0.5 px-1 py-2">
      <div className="flex items-center justify-between px-2 pb-1">
        <span className="text-2xs font-semibold uppercase tracking-[0.05em] text-muted-foreground">
          File Browser
        </span>
        <div className="flex items-center gap-0.5">
          <Button variant="ghost" size="icon-xs" onClick={revealActive} disabled={!activeFilePath} aria-label="Reveal active file" title="Reveal active file"><LocateFixed /></Button>
          <Button
            variant="ghost"
            size="icon-xs"
            disabled={viewerMode}
            onClick={() => { if (!useLayoutStore.getState().viewerMode) openCreateFlowgroupDialog() }}
            aria-label="New flowgroup"
            title="New flowgroup"
            className="text-muted-foreground"
          >
            <Boxes />
          </Button>
          <Button
            variant="ghost"
            size="icon-xs"
            disabled={viewerMode}
            onClick={() => startCreate()}
            aria-label="New file"
            title="New file"
            className="text-muted-foreground"
          >
            <FilePlus2 />
          </Button>
        </div>
      </div>

      {creating && (
        <div className="space-y-1 px-2 pb-2">
          <label htmlFor="new-file-path" className="text-xs text-muted-foreground">{duplicateContent === null ? 'New file path' : 'Duplicate destination'}</label>
          <Input
            id="new-file-path"
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
            className="h-7 px-2 font-mono text-xs md:text-xs"
          />
          <div className="flex gap-1"><Button size="sm" disabled={submitting || !newPath.trim() || viewerMode} onClick={() => { void submitCreate() }}>{submitting ? 'Creating…' : 'Create'}</Button><Button size="sm" variant="ghost" disabled={submitting} onClick={cancelCreate}>Cancel</Button></div>
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
          onDelete={(path) => { if (canMutate(path)) setPendingDelete(path) }}
          canMutate={canMutate} onNewHere={startCreate} onDuplicate={(path) => { void startDuplicate(path) }}
          onOpenSource={openSource} onOpenConfig={(path) => { void openWorkspaceFile(path, { source: false }) }}
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
