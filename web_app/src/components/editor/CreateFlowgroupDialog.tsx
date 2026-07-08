import { useCallback, useMemo, useState } from 'react'
import { Plus, X } from 'lucide-react'
import { useUIStore } from '../../store/uiStore'
import { usePipelines } from '../../hooks/usePipelines'
import { useFlowgroups } from '../../hooks/useFlowgroups'
import { useFileList } from '../../hooks/useFiles'
import { Button } from '../ui/button'
import { Dialog, DialogContent, DialogTitle } from '../ui/dialog'
import { Input } from '../ui/input'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '../ui/select'
import type { FileNode } from '../../types/api'

const NAME_PATTERN = /^[a-zA-Z0-9_-]+$/

/** Sentinel for the "(pipeline root)" subdirectory entry — Radix Select
 * forbids an empty-string item value. Directory names come from the file
 * tree, so this can never collide with a real subfolder. */
const PIPELINE_ROOT = '__pipeline_root__'

/** Walk the recursive file tree to the directory node at `path` (project-
 * relative, `/`-separated). Returns null if the path is absent or is a file. */
function findDirNode(root: FileNode | undefined, path: string): FileNode | null {
  if (!root) return null
  if (!path) return root.type === 'directory' ? root : null
  let node: FileNode = root
  for (const segment of path.split('/')) {
    const child = node.children?.find((c) => c.name === segment)
    if (!child || child.type !== 'directory') return null
    node = child
  }
  return node
}

/** Outer shell: owns open/close, fetches data, and remounts the form on each
 * open. When closed it renders nothing, so the form's `useState` initializers
 * re-run on the next open — this is how the form resets its fields each time
 * the dialog opens (no reset effect needed). */
export function CreateFlowgroupDialog() {
  const open = useUIStore((s) => s.createFlowgroupDialog)
  const close = useUIStore((s) => s.closeCreateFlowgroupDialog)
  const openEditorCreate = useUIStore((s) => s.openFlowgroupEditorCreate)

  const { data: pipelineData } = usePipelines()
  const { data: flowgroupData } = useFlowgroups()
  const { data: fileTree, isLoading: loadingDirs } = useFileList()

  if (!open) return null

  const pipelines = pipelineData?.pipelines.map((p) => p.name) ?? []
  const existingNames = new Set(
    flowgroupData?.flowgroups.map((f) => f.name.toLowerCase()) ?? [],
  )

  return (
    <Dialog
      open
      onOpenChange={(o) => {
        if (!o) close()
      }}
    >
      <DialogContent
        showCloseButton={false}
        aria-describedby={undefined}
        className="gap-0 overflow-hidden p-0 sm:max-w-md"
      >
        <CreateFlowgroupForm
          pipelines={pipelines}
          existingNames={existingNames}
          fileTree={fileTree}
          loadingDirs={loadingDirs}
          close={close}
          openEditorCreate={openEditorCreate}
        />
      </DialogContent>
    </Dialog>
  )
}

interface CreateFlowgroupFormProps {
  pipelines: string[]
  existingNames: Set<string>
  fileTree: FileNode | undefined
  loadingDirs: boolean
  close: () => void
  openEditorCreate: (name: string, pipeline: string, path: string) => void
}

function CreateFlowgroupForm({
  pipelines,
  existingNames,
  fileTree,
  loadingDirs,
  close,
  openEditorCreate,
}: CreateFlowgroupFormProps) {
  // Lazy initializers seed the form from the first available pipeline at mount.
  // Because the shell unmounts this form when the dialog closes, these run
  // afresh on every open — replacing the former "reset on open" effect.
  const [pipeline, setPipeline] = useState(() => pipelines[0] ?? '')
  const [newPipeline, setNewPipeline] = useState('')
  const [isNewPipeline, setIsNewPipeline] = useState(false)
  const [name, setName] = useState('')
  const [selectedSubdir, setSelectedSubdir] = useState('')
  const [newSubdir, setNewSubdir] = useState('')
  const [isNewSubdir, setIsNewSubdir] = useState(false)

  const activePipeline = isNewPipeline ? newPipeline : pipeline

  // Subdirectories of the selected pipeline, derived from the full file tree
  // (the backend no longer lists a directory's contents per-path — the whole
  // tree arrives in one query).
  const subdirs = useMemo(() => {
    if (!activePipeline) return []
    const dir = findDirNode(fileTree, `pipelines/${activePipeline}`)
    return (dir?.children ?? [])
      .filter((c) => c.type === 'directory')
      .map((c) => c.name)
  }, [fileTree, activePipeline])

  // Compute final path
  const activeSubdir = isNewSubdir ? newSubdir : selectedSubdir
  const computedPath = useMemo(() => {
    if (!activePipeline || !name) return ''
    const parts = ['pipelines', activePipeline]
    if (activeSubdir) parts.push(activeSubdir)
    parts.push(`${name}.yaml`)
    return parts.join('/')
  }, [activePipeline, activeSubdir, name])

  // Validation
  const nameError = useMemo(() => {
    if (!name) return ''
    if (!NAME_PATTERN.test(name)) return 'Only letters, numbers, hyphens, and underscores'
    if (existingNames.has(name.toLowerCase())) return 'A flowgroup with this name already exists'
    return ''
  }, [name, existingNames])

  const pipelineError = useMemo(() => {
    if (isNewPipeline && newPipeline && !NAME_PATTERN.test(newPipeline))
      return 'Only letters, numbers, hyphens, and underscores'
    return ''
  }, [isNewPipeline, newPipeline])

  const canCreate = !!activePipeline && !!name && !nameError && !pipelineError

  const handleCreate = useCallback(() => {
    if (!canCreate) return
    openEditorCreate(name, activePipeline, computedPath)
  }, [canCreate, name, activePipeline, computedPath, openEditorCreate])

  return (
    <>
      {/* Header */}
      <div className="flex items-center justify-between border-b border-border px-5 py-3">
        <div>
          <span className="text-2xs font-semibold uppercase tracking-[0.05em] text-muted-foreground">
            New
          </span>
          <DialogTitle className="text-sm font-semibold text-foreground">
            Create Flowgroup
          </DialogTitle>
        </div>
        <Button
          variant="ghost"
          size="icon-sm"
          className="text-muted-foreground"
          aria-label="Close"
          onClick={close}
        >
          <X aria-hidden="true" />
        </Button>
      </div>

      {/* Body */}
      <div className="space-y-4 px-5 py-4">
        {/* Pipeline selector */}
        <div>
          <label className="mb-1 block text-xs font-medium text-muted-foreground">Pipeline</label>
          {isNewPipeline ? (
            <div className="flex items-center gap-2">
              <Input
                value={newPipeline}
                onChange={(e) => setNewPipeline(e.target.value)}
                placeholder="New pipeline name"
                className="h-8 flex-1"
                autoFocus
              />
              <Button
                variant="ghost"
                size="xs"
                onClick={() => {
                  setIsNewPipeline(false)
                  setNewPipeline('')
                }}
              >
                Cancel
              </Button>
            </div>
          ) : (
            <div className="flex items-center gap-2">
              <Select value={pipeline} onValueChange={setPipeline} disabled={pipelines.length === 0}>
                <SelectTrigger size="sm" className="w-full flex-1">
                  <SelectValue
                    placeholder={pipelines.length === 0 ? 'No pipelines found' : 'Select pipeline'}
                  />
                </SelectTrigger>
                <SelectContent>
                  {pipelines.map((p) => (
                    <SelectItem key={p} value={p}>
                      {p}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
              <Button variant="ghost" size="xs" onClick={() => setIsNewPipeline(true)}>
                <Plus aria-hidden="true" />
                New
              </Button>
            </div>
          )}
          {pipelineError && <p className="mt-1 text-xs text-destructive">{pipelineError}</p>}
        </div>

        {/* Flowgroup name */}
        <div>
          <label className="mb-1 block text-xs font-medium text-muted-foreground">
            Flowgroup name
          </label>
          <Input
            value={name}
            onChange={(e) => setName(e.target.value)}
            placeholder="e.g. customer_orders"
            className="h-8 w-full"
            aria-invalid={!!nameError}
          />
          {nameError && <p className="mt-1 text-xs text-destructive">{nameError}</p>}
        </div>

        {/* Directory picker */}
        {activePipeline && (
          <div>
            <label className="mb-1 block text-xs font-medium text-muted-foreground">
              Directory
              <span className="ml-1 font-normal text-muted-foreground/70">(optional subfolder)</span>
            </label>
            {loadingDirs ? (
              <div className="flex items-center gap-2 py-1 text-xs text-muted-foreground">
                <div className="h-3 w-3 animate-spin rounded-full border border-border border-t-muted-foreground" />
                Loading...
              </div>
            ) : isNewSubdir ? (
              <div className="flex items-center gap-2">
                <Input
                  value={newSubdir}
                  onChange={(e) => setNewSubdir(e.target.value)}
                  placeholder="New subfolder name"
                  className="h-8 flex-1"
                />
                <Button
                  variant="ghost"
                  size="xs"
                  onClick={() => {
                    setIsNewSubdir(false)
                    setNewSubdir('')
                  }}
                >
                  Cancel
                </Button>
              </div>
            ) : (
              <div className="flex items-center gap-2">
                <Select
                  value={selectedSubdir === '' ? PIPELINE_ROOT : selectedSubdir}
                  onValueChange={(v) => setSelectedSubdir(v === PIPELINE_ROOT ? '' : v)}
                >
                  <SelectTrigger size="sm" className="w-full flex-1">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value={PIPELINE_ROOT}>(pipeline root)</SelectItem>
                    {subdirs.map((d) => (
                      <SelectItem key={d} value={d}>
                        {d}/
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
                <Button variant="ghost" size="xs" onClick={() => setIsNewSubdir(true)}>
                  <Plus aria-hidden="true" />
                  New
                </Button>
              </div>
            )}
          </div>
        )}

        {/* Path preview */}
        {computedPath && (
          <div className="rounded-md border border-border bg-muted/50 px-3 py-2">
            <span className="text-2xs font-semibold uppercase tracking-[0.05em] text-muted-foreground">
              File path
            </span>
            <p className="mt-0.5 font-mono text-xs text-foreground">{computedPath}</p>
          </div>
        )}
      </div>

      {/* Footer */}
      <div className="flex items-center justify-end gap-2 border-t border-border px-5 py-3">
        <Button variant="ghost" size="sm" onClick={close}>
          Cancel
        </Button>
        <Button size="sm" onClick={handleCreate} disabled={!canCreate}>
          Create
        </Button>
      </div>
    </>
  )
}
