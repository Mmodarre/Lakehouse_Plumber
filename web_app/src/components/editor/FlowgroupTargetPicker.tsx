import { useEffect, useMemo, useState } from 'react'
import { Plus } from 'lucide-react'
import { Button } from '../ui/button'
import { Input } from '../ui/input'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '../ui/select'
import type { FileNode } from '../../types/api'
import { flowgroupFilePath, validateName } from './newFlowgroupDoc'

// ── FlowgroupTargetPicker — where the new flowgroup lands ────
//
// Pipeline (existing or new) + flowgroup name + optional subfolder, with a
// live file-path preview. Owns its own field state and reports the resolved
// target (pipeline, name, path, validity) up to the create dialog via
// `onChange`. Used for blank + template modes; blueprint instances have no
// pipeline/flowgroup of their own and use a different input.

/** Sentinel for the "(pipeline root)" entry — Radix Select forbids "". */
const PIPELINE_ROOT = '__pipeline_root__'

export interface FlowgroupTarget {
  pipeline: string
  name: string
  path: string
  valid: boolean
}

export interface FlowgroupTargetPickerProps {
  pipelines: string[]
  /**
   * Lowercased existing flowgroup names keyed by pipeline. LHP keys flowgroup
   * uniqueness on (pipeline, flowgroup), so the collision guard is scoped to
   * the SELECTED pipeline — the same name in another pipeline is legitimate.
   */
  existingNamesByPipeline: Map<string, Set<string>>
  fileTree: FileNode | undefined
  loadingDirs: boolean
  /** Pre-selected pipeline (e.g. from the pipeline drill modal). */
  seedPipeline?: string
  onChange: (target: FlowgroupTarget) => void
  disabled?: boolean
}

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

export function FlowgroupTargetPicker({
  pipelines,
  existingNamesByPipeline,
  fileTree,
  loadingDirs,
  seedPipeline,
  onChange,
  disabled,
}: FlowgroupTargetPickerProps) {
  const [pipeline, setPipeline] = useState(() => seedPipeline ?? pipelines[0] ?? '')
  const [newPipeline, setNewPipeline] = useState('')
  const [isNewPipeline, setIsNewPipeline] = useState(
    () => seedPipeline === undefined && pipelines.length === 0,
  )
  const [name, setName] = useState('')
  const [selectedSubdir, setSelectedSubdir] = useState('')
  const [newSubdir, setNewSubdir] = useState('')
  const [isNewSubdir, setIsNewSubdir] = useState(false)

  const activePipeline = isNewPipeline ? newPipeline : pipeline
  const activeSubdir = isNewSubdir ? newSubdir : selectedSubdir

  const subdirs = useMemo(() => {
    if (!activePipeline) return []
    const dir = findDirNode(fileTree, `pipelines/${activePipeline}`)
    return (dir?.children ?? []).filter((c) => c.type === 'directory').map((c) => c.name)
  }, [fileTree, activePipeline])

  const nameError = useMemo(() => {
    if (!name) return ''
    const invalid = validateName(name)
    if (invalid) return invalid
    // Scoped to the selected pipeline — a new pipeline has no existing names.
    const inPipeline = existingNamesByPipeline.get(activePipeline)
    if (inPipeline?.has(name.toLowerCase())) {
      return 'A flowgroup with this name already exists in this pipeline'
    }
    return ''
  }, [name, existingNamesByPipeline, activePipeline])

  const pipelineError = useMemo(() => {
    if (isNewPipeline && newPipeline) return validateName(newPipeline) ?? ''
    return ''
  }, [isNewPipeline, newPipeline])

  const subdirError = useMemo(() => {
    if (isNewSubdir && newSubdir) return validateName(newSubdir) ?? ''
    return ''
  }, [isNewSubdir, newSubdir])

  const path = useMemo(
    () => (activePipeline && name ? flowgroupFilePath(activePipeline, activeSubdir, name) : ''),
    [activePipeline, activeSubdir, name],
  )
  const valid = !!activePipeline && !!name && !nameError && !pipelineError && !subdirError

  // Report the resolved target upward whenever it changes.
  useEffect(() => {
    onChange({ pipeline: activePipeline, name, path, valid })
  }, [activePipeline, name, path, valid, onChange])

  return (
    <div className="space-y-4">
      {/* Pipeline */}
      <div>
        <label className="mb-1 block text-xs font-medium text-muted-foreground">Pipeline</label>
        {isNewPipeline ? (
          <div className="flex items-center gap-2">
            <Input
              value={newPipeline}
              onChange={(e) => setNewPipeline(e.target.value)}
              placeholder="New pipeline name"
              className="h-8 flex-1"
              disabled={disabled}
              autoFocus
            />
            {pipelines.length > 0 && (
              <Button
                variant="ghost"
                size="xs"
                disabled={disabled}
                onClick={() => {
                  setIsNewPipeline(false)
                  setNewPipeline('')
                }}
              >
                Cancel
              </Button>
            )}
          </div>
        ) : (
          <div className="flex items-center gap-2">
            <Select value={pipeline} onValueChange={setPipeline} disabled={disabled}>
              <SelectTrigger size="sm" className="w-full flex-1">
                <SelectValue placeholder="Select pipeline" />
              </SelectTrigger>
              <SelectContent>
                {pipelines.map((p) => (
                  <SelectItem key={p} value={p}>
                    {p}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
            <Button
              variant="ghost"
              size="xs"
              disabled={disabled}
              onClick={() => setIsNewPipeline(true)}
              aria-label="New pipeline"
            >
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
          disabled={disabled}
          aria-invalid={!!nameError}
        />
        {nameError && <p className="mt-1 text-xs text-destructive">{nameError}</p>}
      </div>

      {/* Subfolder */}
      {activePipeline && (
        <div>
          <label className="mb-1 block text-xs font-medium text-muted-foreground">
            Directory
            <span className="ml-1 font-normal text-muted-foreground/70">(optional subfolder)</span>
          </label>
          {loadingDirs ? (
            <div className="flex items-center gap-2 py-1 text-xs text-muted-foreground">
              <div className="size-3 animate-spin rounded-full border border-border border-t-muted-foreground" />
              Loading…
            </div>
          ) : isNewSubdir ? (
            <div className="flex items-center gap-2">
              <Input
                value={newSubdir}
                onChange={(e) => setNewSubdir(e.target.value)}
                placeholder="New subfolder name"
                className="h-8 flex-1"
                disabled={disabled}
              />
              <Button
                variant="ghost"
                size="xs"
                disabled={disabled}
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
                disabled={disabled}
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
              <Button
                variant="ghost"
                size="xs"
                disabled={disabled}
                onClick={() => setIsNewSubdir(true)}
                aria-label="New subfolder"
              >
                <Plus aria-hidden="true" />
                New
              </Button>
            </div>
          )}
          {subdirError && <p className="mt-1 text-xs text-destructive">{subdirError}</p>}
        </div>
      )}

      {path && <PathPreview path={path} />}
    </div>
  )
}

export function PathPreview({ path }: { path: string }) {
  return (
    <div className="rounded-md border border-border bg-muted/50 px-3 py-2">
      <span className="text-2xs font-semibold uppercase tracking-[0.05em] text-muted-foreground">
        File path
      </span>
      <p className="mt-0.5 break-all font-mono text-xs text-foreground">{path}</p>
    </div>
  )
}
