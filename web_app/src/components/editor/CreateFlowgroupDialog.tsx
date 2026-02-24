import { useCallback, useEffect, useMemo, useState } from 'react'
import { useUIStore } from '../../store/uiStore'
import { usePipelines } from '../../hooks/usePipelines'
import { useFlowgroups } from '../../hooks/useFlowgroups'
import { fetchFilePath } from '../../api/files'

const NAME_PATTERN = /^[a-zA-Z0-9_-]+$/

export function CreateFlowgroupDialog() {
  const open = useUIStore((s) => s.createFlowgroupDialog)
  const close = useUIStore((s) => s.closeCreateFlowgroupDialog)
  const openEditorCreate = useUIStore((s) => s.openFlowgroupEditorCreate)

  const { data: pipelineData } = usePipelines()
  const { data: flowgroupData } = useFlowgroups()

  const [pipeline, setPipeline] = useState('')
  const [newPipeline, setNewPipeline] = useState('')
  const [isNewPipeline, setIsNewPipeline] = useState(false)
  const [name, setName] = useState('')
  const [subdirs, setSubdirs] = useState<string[]>([])
  const [selectedSubdir, setSelectedSubdir] = useState('')
  const [newSubdir, setNewSubdir] = useState('')
  const [isNewSubdir, setIsNewSubdir] = useState(false)
  const [loadingDirs, setLoadingDirs] = useState(false)

  const pipelines = useMemo(
    () => pipelineData?.pipelines.map((p) => p.name) ?? [],
    [pipelineData],
  )

  const existingNames = useMemo(
    () => new Set(flowgroupData?.flowgroups.map((f) => f.name.toLowerCase()) ?? []),
    [flowgroupData],
  )

  const activePipeline = isNewPipeline ? newPipeline : pipeline

  // Fetch subdirectories when pipeline changes
  useEffect(() => {
    if (!activePipeline || !open) {
      setSubdirs([])
      return
    }
    setLoadingDirs(true)
    fetchFilePath(`pipelines/${activePipeline}`)
      .then((res) => {
        const dirs = (res.items ?? [])
          .filter((item) => item.type === 'directory')
          .map((item) => item.name)
        setSubdirs(dirs)
      })
      .catch(() => setSubdirs([]))
      .finally(() => setLoadingDirs(false))
  }, [activePipeline, open])

  // Reset form when dialog opens
  useEffect(() => {
    if (open) {
      setPipeline(pipelines[0] ?? '')
      setNewPipeline('')
      setIsNewPipeline(false)
      setName('')
      setSelectedSubdir('')
      setNewSubdir('')
      setIsNewSubdir(false)
    }
  }, [open, pipelines])

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

  // Escape to close
  useEffect(() => {
    if (!open) return
    const handleKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape') close()
    }
    document.addEventListener('keydown', handleKey)
    return () => document.removeEventListener('keydown', handleKey)
  }, [open, close])

  if (!open) return null

  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-black/30"
      onClick={(e) => {
        if (e.target === e.currentTarget) close()
      }}
    >
      <div className="w-full max-w-md overflow-hidden rounded-lg border border-slate-200 bg-white shadow-xl">
        {/* Header */}
        <div className="flex items-center justify-between border-b border-slate-200 px-5 py-3">
          <div>
            <span className="text-[10px] font-semibold uppercase tracking-wider text-slate-400">
              New
            </span>
            <h2 className="text-sm font-semibold text-slate-800">Create Flowgroup</h2>
          </div>
          <button
            onClick={close}
            className="rounded p-1.5 text-slate-400 hover:bg-slate-100 hover:text-slate-600"
          >
            <svg className="h-5 w-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
            </svg>
          </button>
        </div>

        {/* Body */}
        <div className="space-y-4 px-5 py-4">
          {/* Pipeline selector */}
          <div>
            <label className="mb-1 block text-xs font-medium text-slate-600">Pipeline</label>
            {isNewPipeline ? (
              <div className="flex items-center gap-2">
                <input
                  type="text"
                  value={newPipeline}
                  onChange={(e) => setNewPipeline(e.target.value)}
                  placeholder="New pipeline name"
                  className="flex-1 rounded border border-slate-300 px-2.5 py-1.5 text-sm text-slate-800 focus:border-blue-400 focus:outline-none focus:ring-1 focus:ring-blue-400"
                  autoFocus
                />
                <button
                  onClick={() => {
                    setIsNewPipeline(false)
                    setNewPipeline('')
                  }}
                  className="text-xs text-slate-400 hover:text-slate-600"
                >
                  Cancel
                </button>
              </div>
            ) : (
              <div className="flex items-center gap-2">
                <select
                  value={pipeline}
                  onChange={(e) => setPipeline(e.target.value)}
                  className="flex-1 rounded border border-slate-300 px-2.5 py-1.5 text-sm text-slate-800 focus:border-blue-400 focus:outline-none focus:ring-1 focus:ring-blue-400"
                >
                  {pipelines.length === 0 && (
                    <option value="" disabled>No pipelines found</option>
                  )}
                  {pipelines.map((p) => (
                    <option key={p} value={p}>{p}</option>
                  ))}
                </select>
                <button
                  onClick={() => setIsNewPipeline(true)}
                  className="whitespace-nowrap text-xs text-blue-600 hover:text-blue-700"
                >
                  + New
                </button>
              </div>
            )}
            {pipelineError && (
              <p className="mt-1 text-xs text-red-500">{pipelineError}</p>
            )}
          </div>

          {/* Flowgroup name */}
          <div>
            <label className="mb-1 block text-xs font-medium text-slate-600">Flowgroup name</label>
            <input
              type="text"
              value={name}
              onChange={(e) => setName(e.target.value)}
              placeholder="e.g. customer_orders"
              className="w-full rounded border border-slate-300 px-2.5 py-1.5 text-sm text-slate-800 focus:border-blue-400 focus:outline-none focus:ring-1 focus:ring-blue-400"
            />
            {nameError && (
              <p className="mt-1 text-xs text-red-500">{nameError}</p>
            )}
          </div>

          {/* Directory picker */}
          {activePipeline && (
            <div>
              <label className="mb-1 block text-xs font-medium text-slate-600">
                Directory
                <span className="ml-1 font-normal text-slate-400">(optional subfolder)</span>
              </label>
              {loadingDirs ? (
                <div className="flex items-center gap-2 py-1 text-xs text-slate-400">
                  <div className="h-3 w-3 animate-spin rounded-full border border-slate-300 border-t-slate-500" />
                  Loading...
                </div>
              ) : isNewSubdir ? (
                <div className="flex items-center gap-2">
                  <input
                    type="text"
                    value={newSubdir}
                    onChange={(e) => setNewSubdir(e.target.value)}
                    placeholder="New subfolder name"
                    className="flex-1 rounded border border-slate-300 px-2.5 py-1.5 text-sm text-slate-800 focus:border-blue-400 focus:outline-none focus:ring-1 focus:ring-blue-400"
                  />
                  <button
                    onClick={() => {
                      setIsNewSubdir(false)
                      setNewSubdir('')
                    }}
                    className="text-xs text-slate-400 hover:text-slate-600"
                  >
                    Cancel
                  </button>
                </div>
              ) : (
                <div className="flex items-center gap-2">
                  <select
                    value={selectedSubdir}
                    onChange={(e) => setSelectedSubdir(e.target.value)}
                    className="flex-1 rounded border border-slate-300 px-2.5 py-1.5 text-sm text-slate-800 focus:border-blue-400 focus:outline-none focus:ring-1 focus:ring-blue-400"
                  >
                    <option value="">(pipeline root)</option>
                    {subdirs.map((d) => (
                      <option key={d} value={d}>{d}/</option>
                    ))}
                  </select>
                  <button
                    onClick={() => setIsNewSubdir(true)}
                    className="whitespace-nowrap text-xs text-blue-600 hover:text-blue-700"
                  >
                    + New
                  </button>
                </div>
              )}
            </div>
          )}

          {/* Path preview */}
          {computedPath && (
            <div className="rounded border border-slate-200 bg-slate-50 px-3 py-2">
              <span className="text-[10px] font-medium uppercase tracking-wider text-slate-400">
                File path
              </span>
              <p className="mt-0.5 font-mono text-xs text-slate-700">{computedPath}</p>
            </div>
          )}
        </div>

        {/* Footer */}
        <div className="flex items-center justify-end gap-2 border-t border-slate-200 px-5 py-3">
          <button
            onClick={close}
            className="rounded px-3 py-1.5 text-xs font-medium text-slate-600 hover:bg-slate-100"
          >
            Cancel
          </button>
          <button
            onClick={handleCreate}
            disabled={!canCreate}
            className="rounded bg-blue-600 px-4 py-1.5 text-xs font-medium text-white hover:bg-blue-700 disabled:opacity-40"
          >
            Create
          </button>
        </div>
      </div>
    </div>
  )
}
