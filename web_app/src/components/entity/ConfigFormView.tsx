import { useEffect, useMemo } from 'react'
import { TriangleAlert } from 'lucide-react'
import type { ConfigFileHandle } from '../../lib/yaml-doc'
import { useDocumentStore, useEntityDocument } from '../../store/documentStore'
import type { ConfigKind, DocKind } from '../../store/workspaceStore'
import { useWorkspaceStore } from '../../store/workspaceStore'
import { JobConfigEditor } from '../config/job/JobConfigEditor'
import { PipelineConfigEditor } from '../config/pipeline/PipelineConfigEditor'
import { ProjectConfigForm } from '../config/project/ProjectConfigForm'
import type { ConfigDocSource } from '../config/shared/docFormSupport'

// ── ConfigFormView — a config surface re-hosted on the document core ──
//
// The Form view of a config center tab (project / pipeline / job). It
// replaces the retired ConfigurationPage + useConfigFile lifecycle: the
// entity document core (store/documentStore) owns the comment-preserving
// parse handle keyed by `version`, saving is the YAML buffer's ⌘S path
// (useWorkspaceSave), and there is no SaveBar / conflict dialog / external-
// change banner here. This component owns what ConfigPageShell's body used
// to (the scroll frame + max-w-3xl column), plus the degraded banner and
// the viewer/parse-error read-only wrapper; the surviving hand-JSX section
// editors render inside, bound to documentStore via ConfigDocSource.
//
// One field commit = one atomic yaml-doc op → one documentStore.mutate →
// one byte-surgical serialize pushed into the buffer (comments preserved).
// Reads/re-renders key on documentStore `version` ONLY (the handle identity
// is preserved across mutate). Degraded (buffer has parse errors): the
// last-good handle is kept, form editing pauses, the banner points at the
// YAML view. Viewer lens (layoutStore.viewerMode): fields are inert and
// mutate is a no-op.

const CONFIG_KIND_TO_DOC_KIND: Record<ConfigKind, DocKind> = {
  project: 'project',
  pipeline: 'pipeline_config',
  job: 'job_config',
}

export interface ConfigFormViewProps {
  /** Strip-wide id of the hosting config tab (`config:<path>`). */
  tabId: string
  /** Project-relative path of the config file this tab edits. */
  path: string
  /** Which config surface (chooses the DocKind + section editor). */
  configKind: ConfigKind
}

export function ConfigFormView({ tabId, path, configKind }: ConfigFormViewProps) {
  const docKind = CONFIG_KIND_TO_DOC_KIND[configKind]

  // Parse the current buffer into a documentStore handle. Idempotent: `open`
  // no-ops when a document of the same kind is already open at `path`, and
  // useEntityDocument reparses once the buffer content actually arrives.
  useEffect(() => {
    useDocumentStore.getState().open(path, docKind)
  }, [path, docKind])

  const entity = useEntityDocument(path)
  const buffer = useWorkspaceStore((s) => s.buffers.find((b) => b.path === path) ?? null)

  // The ConfigDocSource the section editors read. Rebuilt on `version` (the
  // only memo key — never the handle, whose identity is preserved across
  // mutate) so version-keyed section memos recompute after every edit.
  const source: ConfigDocSource = useMemo(
    () => ({
      path,
      isLoading: entity.handle === null && !(buffer?.loadFailed ?? false),
      loadError: buffer?.loadFailed ? 'Failed to load file' : null,
      handle: entity.handle as ConfigFileHandle | null,
      errors: entity.errors,
      version: entity.version,
      mutate: (fn) => {
        // Viewer/degraded/loading ⇒ no write (documentStore.mutate also
        // refuses on parse errors / no handle; this covers viewer too).
        if (entity.readOnly) return
        useDocumentStore.getState().mutate(path, fn)
      },
    }),
    [path, entity.handle, entity.errors, entity.version, entity.readOnly, buffer?.loadFailed],
  )

  const editor =
    configKind === 'project' ? (
      <ProjectConfigForm file={source} />
    ) : configKind === 'pipeline' ? (
      <PipelineConfigEditor file={source} />
    ) : (
      <JobConfigEditor file={source} />
    )

  const degraded = entity.readOnlyReason === 'parse-error'

  return (
    <div className="flex min-h-0 flex-1 flex-col" data-testid="config-form-view" data-tab-id={tabId}>
      {degraded && (
        <div
          role="alert"
          data-testid="config-degraded-banner"
          className="flex items-center gap-2 border-b border-warning/40 bg-warning/10 px-6 py-2 text-xs text-warning"
        >
          <TriangleAlert className="size-3.5 shrink-0" aria-hidden="true" />
          <span>
            This file has YAML errors — form editing is paused and your last valid state is
            kept. Switch to the YAML view to fix it.
          </span>
        </div>
      )}
      <div className="min-h-0 flex-1 overflow-y-auto px-6 py-4">
        <div
          className="mx-auto w-full min-w-0 max-w-3xl space-y-4 aria-disabled:opacity-60"
          inert={entity.readOnly ? true : undefined}
          aria-disabled={entity.readOnly || undefined}
          data-viewer={entity.readOnlyReason === 'viewer' || undefined}
        >
          {editor}
        </div>
      </div>
    </div>
  )
}
