import { lazy, Suspense, useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { AlertTriangle, FileCheck, FileCode2, FileText, Loader2, Tags } from 'lucide-react'
import { fetchFileContentWithMeta } from '../../../api/files'
import { fetchFlowgroupRelatedFiles } from '../../../api/flowgroups'
import { useFileList } from '../../../hooks/useFiles'
import { errorMessage } from '../../../lib/errors'
import {
  collectFilePaths,
  generatedPythonCandidates,
  resolveArtifactPaths,
  type ArtifactRef,
} from '../../../lib/artifactPaths'
import { cn } from '../../../lib/utils'
import { useUIStore } from '../../../store/uiStore'
import { useDocumentStore } from '../../../store/documentStore'
import { useRunStore } from '../../../store/runStore'
import { isReadOnlyPath, useWorkspaceStore, workspaceTabId } from '../../../store/workspaceStore'
import { isYamlPath } from '../../editor/yamlSaveSupport'
import { ConflictDialog } from '../../editor/ConflictDialog'
import type { MonacoEditorHandle } from '../../editor/MonacoEditorWrapper'
import { EmptyState } from '../../common/EmptyState'
import { loadBufferContent } from '../../workspace/flowgroupBuffers'
import { useWorkspaceSave } from '../../workspace/useWorkspaceSave'
import { YamlView } from './YamlView'

// ── CodeView — multi-file Code surface (§6.2) ────────────────
//
// The 'code' view of a flowgroup entity tab: an artifact sub-tab strip over a
// single Monaco area. Tab 1 is the source `<flowgroup>.yaml`, EDITABLE through
// the same YamlView + useWorkspaceSave + documentStore + workspace-buffer
// pipeline the entity used before (so dirty-tracking / debounced capture /
// ⌘S save / 412-conflict / flush-on-switch behave identically). The remaining
// tabs are READ-ONLY Monaco models over the generated Python and any source
// SQL / schema the flowgroup references — each with a muted chip.
//
// This view owns its OWN editor machinery rather than CenterArea's: CenterArea
// renders <CodeView/> (not YamlView) for the code view, so its shared
// `editorRef` is never mounted here. The store transitions that would unmount
// the editor (tab switch, Code→Graph) are flushed via the subscription below,
// mirroring CenterArea's flush-on-switch.

const MonacoEditorWrapper = lazy(() => import('../../editor/MonacoEditorWrapper'))

const CAPTURE_DEBOUNCE_MS = 500

/** Props contract consumed by CenterArea. Identity-only — the view re-derives
 * its artifacts from the flowgroup at `filePath` for the active env. */
export interface CodeViewProps {
  /** Strip-wide tab id (used to detect this tab losing focus). */
  tabId: string
  /** '' for a template (which has no pipeline). */
  pipeline: string
  /** Flowgroup name, or the template name in template mode. */
  flowgroup: string
  /** Project-relative path of the source YAML (the editable artifact tab). */
  filePath: string
}

/** An artifact shown in the strip. `missing` marks a generated-Python tab whose
 * file isn't on disk yet (rendered as a "not generated" hint, not a model). */
type DisplayArtifact = ArtifactRef & { missing?: boolean }

function iconFor(kind: ArtifactRef['kind']) {
  switch (kind) {
    case 'generated-python':
    case 'source-python':
    case 'source-sql':
      return FileCode2
    case 'source-expectations':
      return FileCheck
    case 'source-tags':
      return Tags
    default:
      return FileText
  }
}

function ArtifactSkeleton() {
  return (
    <div className="flex min-h-0 flex-1 items-center justify-center bg-card">
      <Loader2 className="size-5 animate-spin text-muted-foreground" aria-hidden="true" />
    </div>
  )
}

function CenteredState({ children }: { children: React.ReactNode }) {
  return <div className="flex min-h-0 flex-1 items-center justify-center p-6">{children}</div>
}

/** A read-only artifact pane: fetches the file's on-disk content and mounts a
 * read-only Monaco model. Loading / fetch-error states are handled without
 * crashing the surrounding strip. */
function ReadOnlyArtifactPane({ path }: { path: string }) {
  const query = useQuery({
    queryKey: ['file-content', path],
    queryFn: () => fetchFileContentWithMeta(path),
    staleTime: 30_000,
    retry: false,
  })

  if (query.isPending) return <ArtifactSkeleton />
  if (query.isError) {
    return (
      <CenteredState>
        <EmptyState
          icon={AlertTriangle}
          title="Couldn't load file"
          message={errorMessage(query.error, 'This artifact failed to load.')}
        />
      </CenteredState>
    )
  }
  return (
    <div className="min-h-0 flex-1">
      <Suspense fallback={<ArtifactSkeleton />}>
        <MonacoEditorWrapper
          key={`${path}::${query.data.etag ?? ''}`}
          path={path}
          content={query.data.content}
          readOnly
        />
      </Suspense>
    </div>
  )
}

export function CodeView({ tabId, pipeline, flowgroup, filePath }: CodeViewProps) {
  const env = useUIStore((s) => s.selectedEnv)
  const isFlowgroup = pipeline !== ''

  // ── artifact resolution ─────────────────────────────────────
  const filesQuery = useFileList()
  const relatedQuery = useQuery({
    queryKey: ['flowgroup-related', flowgroup, env],
    queryFn: () => fetchFlowgroupRelatedFiles(flowgroup, env),
    enabled: isFlowgroup && flowgroup !== '',
    retry: false,
  })

  const existingPaths = useMemo(() => collectFilePaths(filesQuery.data), [filesQuery.data])

  const artifacts = useMemo<DisplayArtifact[]>(() => {
    const resolved: DisplayArtifact[] = resolveArtifactPaths(
      {
        pipeline,
        flowgroup,
        sourceFilePath: filePath,
        env,
        related: relatedQuery.data?.related_files,
      },
      existingPaths,
    )
    // Surface a "not generated yet" Python tab even when the file is absent, so
    // the artifact is discoverable and the user knows how to produce it.
    if (
      isFlowgroup &&
      filesQuery.isSuccess &&
      !resolved.some((a) => a.kind === 'generated-python')
    ) {
      const expected = generatedPythonCandidates(env, pipeline, flowgroup)[0]
      if (expected) {
        resolved.push({
          path: expected,
          label: expected.split('/').pop() ?? expected,
          language: 'python',
          kind: 'generated-python',
          editable: false,
          chip: 'generated · read-only',
          missing: true,
        })
      }
    }
    return resolved
  }, [
    pipeline,
    flowgroup,
    filePath,
    env,
    relatedQuery.data,
    existingPaths,
    isFlowgroup,
    filesQuery.isSuccess,
  ])

  // ── active sub-tab ──────────────────────────────────────────
  const [selectedArtifactPath, setSelectedArtifactPath] = useState(filePath)
  // Derive the effective selection so one that no longer resolves (the artifact
  // list changed) falls back to the source yaml — no setState-in-effect.
  const activeArtifactPath = artifacts.some((a) => a.path === selectedArtifactPath)
    ? selectedArtifactPath
    : (artifacts[0]?.path ?? filePath)
  const activeArtifact = artifacts.find((a) => a.path === activeArtifactPath) ?? artifacts[0]
  const yamlActive = activeArtifact?.editable === true

  // ── editable-yaml machinery (source tab only, scoped to filePath) ──
  const editorRef = useRef<MonacoEditorHandle>(null)
  // Mirrors the buffer the mounted Monaco edits: the source yaml when the yaml
  // sub-tab is active, else null (a read-only artifact is shown instead).
  const activePathRef = useRef<string | null>(null)
  const captureTimerRef = useRef<number | null>(null)

  useEffect(() => {
    activePathRef.current = yamlActive ? filePath : null
  }, [yamlActive, filePath])

  const buffer = useWorkspaceStore((s) => s.buffers.find((b) => b.path === filePath))
  const dirtyCount = useWorkspaceStore((s) => s.buffers.filter((b) => b.isDirty).length)
  const anySaving = useWorkspaceStore((s) => s.buffers.some((b) => b.isSaving))
  const setDirty = useWorkspaceStore((s) => s.setDirty)
  // Extracted so the out-of-band reconcile effect (below) keys on the buffer's
  // content/dirty flag without depending on the whole buffer object.
  const bufferContent = buffer?.content
  const bufferIsDirty = buffer?.isDirty

  // Ensure the source buffer exists + is loaded, so the yaml sub-tab works even
  // when CodeView is mounted outside CenterArea's body-buffer effect (tests /
  // direct mounts). Idempotent: openBuffer is no-op-if-open.
  useEffect(() => {
    const ws = useWorkspaceStore.getState()
    if (!ws.buffers.some((b) => b.path === filePath)) {
      ws.openBuffer(filePath, { loading: true, activate: false })
      void loadBufferContent(filePath)
    }
  }, [filePath])

  const cancelCapture = useCallback(() => {
    if (captureTimerRef.current !== null) {
      window.clearTimeout(captureTimerRef.current)
      captureTimerRef.current = null
    }
  }, [])

  const captureActive = useCallback(() => {
    cancelCapture()
    const path = activePathRef.current
    if (path && editorRef.current) {
      const text = editorRef.current.getValue()
      useWorkspaceStore.getState().updateContent(path, text)
      if (useDocumentStore.getState().docs[path]) {
        useDocumentStore.getState().reparse(path, text)
      }
    }
  }, [cancelCapture])

  const scheduleCapture = useCallback(() => {
    if (captureTimerRef.current !== null) window.clearTimeout(captureTimerRef.current)
    captureTimerRef.current = window.setTimeout(() => {
      captureTimerRef.current = null
      captureActive()
    }, CAPTURE_DEBOUNCE_MS)
  }, [captureActive])

  useEffect(() => cancelCapture, [cancelCapture])

  // Flush the live editor into the store right before unload (sync writes).
  useEffect(() => {
    window.addEventListener('beforeunload', captureActive)
    return () => window.removeEventListener('beforeunload', captureActive)
  }, [captureActive])

  // Reconcile the mounted editor when the source buffer is reset OUT OF BAND — a
  // session-restore "Discard all", take-theirs, or replace that rewrites
  // content→originalContent while THIS view (not CenterArea's YamlView) owns the
  // editor. CenterArea's applyDiscard pushes the reverted text through its own
  // editorRef, which is null while CodeView is mounted, so the visible editor
  // would otherwise keep the discarded text and the next keystroke/⌘S would
  // write it back. Only touch a CLEAN buffer whose live text drifted from it:
  // after a normal capture/save getValue() already equals content (no-op), and a
  // dirty buffer is the user's in-flight edit. setValue emits onDirtyChange(false)
  // → cancelCapture, so a pending debounced capture can't resurrect the discard.
  // Mirrors useWorkspaceSave's take-theirs setValue.
  useEffect(() => {
    if (!yamlActive || bufferContent === undefined || bufferIsDirty) return
    const editor = editorRef.current
    if (editor && editor.getValue() !== bufferContent) {
      editor.setValue(bufferContent)
    }
  }, [yamlActive, bufferContent, bufferIsDirty])

  const handleDirtyChange = useCallback(
    (dirty: boolean) => {
      const path = activePathRef.current
      if (!path) return
      if (dirty) {
        setDirty(path, true)
        scheduleCapture()
      } else {
        cancelCapture()
        setDirty(path, false)
      }
    },
    [setDirty, scheduleCapture, cancelCapture],
  )

  const { saveActive, saveAllDirty, conflict, cancelConflict, resolveKeepMine, resolveTakeTheirs } =
    useWorkspaceSave({ editorRef, activePathRef, captureActive })

  const handleEditorMount = useCallback(() => {
    const path = activePathRef.current
    if (!path || !isYamlPath(path)) return
    const issue = useRunStore
      .getState()
      .issues.find((i) => i.code === 'YAML-SYNTAX' && i.file_path === path)
    if (!issue) return
    const line = issue.context['line']
    const column = issue.context['column']
    if (typeof line !== 'number' || typeof column !== 'number') return
    editorRef.current?.setYamlMarkers([{ line, column, message: issue.title }])
  }, [])

  const handleRetryLoad = useCallback((path: string) => {
    void loadBufferContent(path).then(() => {
      const buf = useWorkspaceStore.getState().buffers.find((b) => b.path === path)
      if (buf && !buf.loadFailed && activePathRef.current === path) {
        editorRef.current?.setValue(buf.content)
      }
    })
  }, [])

  // Flush the live yaml editor when a STORE transition removes it from view
  // (this tab deactivated, or a Code→Graph view switch). The subscriber runs
  // synchronously inside setActive/setTabView's set(), before React unmounts
  // this view, so text typed inside the debounce window is never lost. Local
  // sub-tab switches are flushed in selectArtifact instead. Mirrors CenterArea.
  useEffect(() => {
    const shownYamlPath = (
      s: ReturnType<typeof useWorkspaceStore.getState>,
    ): string | null => {
      const tab = s.tabs.find((t) => workspaceTabId(t) === tabId)
      const showing = s.activePath === tabId && tab?.kind === 'entity' && tab.view === 'code'
      return showing ? activePathRef.current : null
    }
    let prev = shownYamlPath(useWorkspaceStore.getState())
    return useWorkspaceStore.subscribe((s) => {
      const next = shownYamlPath(s)
      if (next === prev) return
      const outPath = prev
      prev = next
      if (
        outPath !== null &&
        editorRef.current &&
        activePathRef.current === outPath &&
        s.buffers.some((b) => b.path === outPath)
      ) {
        cancelCapture()
        const text = editorRef.current.getValue()
        useWorkspaceStore.getState().updateContent(outPath, text)
        if (useDocumentStore.getState().docs[outPath]) {
          useDocumentStore.getState().reparse(outPath, text)
        }
      }
    })
  }, [tabId, cancelCapture])

  const selectArtifact = useCallback(
    (path: string) => {
      if (path === activeArtifactPath) return
      // Flush the editable yaml editor before it unmounts on the sub-tab switch.
      if (activePathRef.current) captureActive()
      setSelectedArtifactPath(path)
    },
    [activeArtifactPath, captureActive],
  )

  const renderPane = () => {
    if (!activeArtifact) return <ArtifactSkeleton />
    if (activeArtifact.editable) {
      if (!buffer || buffer.loading) return <ArtifactSkeleton />
      return (
        <YamlView
          buffer={buffer}
          editorRef={editorRef}
          isReadOnly={isReadOnlyPath(buffer.path)}
          dirtyCount={dirtyCount}
          anySaving={anySaving}
          onDirtyChange={handleDirtyChange}
          onSave={saveActive}
          onSaveAll={saveAllDirty}
          onEditorMount={handleEditorMount}
          onRetryLoad={handleRetryLoad}
        />
      )
    }
    if (activeArtifact.missing) {
      return (
        <CenteredState>
          <EmptyState
            icon={FileCode2}
            title="Not generated yet"
            message={`Run Generate to produce ${activeArtifact.label}.`}
          />
        </CenteredState>
      )
    }
    return <ReadOnlyArtifactPane path={activeArtifact.path} />
  }

  return (
    <div className="flex min-h-0 flex-1 flex-col bg-card">
      <div
        role="tablist"
        aria-label="Artifacts"
        className="flex flex-none items-stretch overflow-x-auto border-b border-border bg-sidebar"
      >
        {artifacts.map((a) => {
          const Icon = iconFor(a.kind)
          const active = a.path === activeArtifactPath
          return (
            <button
              key={a.path}
              type="button"
              role="tab"
              aria-selected={active}
              onClick={() => selectArtifact(a.path)}
              className={cn(
                'flex h-[34px] shrink-0 items-center gap-1.5 border-b-2 px-3 text-xs font-medium transition-colors',
                active
                  ? 'border-primary text-foreground'
                  : 'border-transparent text-muted-foreground hover:text-foreground',
              )}
            >
              <Icon className="size-3.5" aria-hidden="true" />
              <span className="truncate">{a.label}</span>
              {a.chip && (
                <span className="rounded-sm bg-muted px-1 text-2xs font-normal text-muted-foreground">
                  {a.chip}
                </span>
              )}
            </button>
          )
        })}
      </div>

      <div className="flex min-h-0 flex-1 flex-col">{renderPane()}</div>

      <ConflictDialog
        conflict={conflict}
        onCancel={cancelConflict}
        onKeepMine={resolveKeepMine}
        onTakeTheirs={resolveTakeTheirs}
      />
    </div>
  )
}
