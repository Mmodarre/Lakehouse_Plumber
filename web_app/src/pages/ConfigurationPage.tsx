import { useEffect, useMemo, useRef, useState } from 'react'
import { Navigate, useNavigate, useParams } from 'react-router-dom'
import { FileCog, FilePlus2 } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { Tabs, TabsList, TabsTrigger } from '@/components/ui/tabs'
import { EmptyState } from '../components/common/EmptyState'
import { DiscardChangesDialog } from '../components/editor/DiscardChangesDialog'
import { ConfigFilePicker } from '../components/config/ConfigFilePicker'
import { ConfigPageShell } from '../components/config/ConfigPageShell'
import { CreateFromTemplateDialog } from '../components/config/CreateFromTemplateDialog'
import { JobConfigEditor } from '../components/config/job/JobConfigEditor'
import { PipelineConfigEditor } from '../components/config/pipeline/PipelineConfigEditor'
import { ProjectConfigForm } from '../components/config/project/ProjectConfigForm'
import { CONFIG_TAB_COPY, listConfigYamlFiles } from '../components/config/configFileSupport'
import type { ConfigTabKind } from '../components/config/configFileSupport'
import { useBeforeUnloadGuard } from '../hooks/useBeforeUnloadGuard'
import { useConfigFile } from '../hooks/useConfigFile'
import { useFileList } from '../hooks/useFiles'
import { useDirtyGuardSource } from '../store/dirtyGuardStore'
import { useUIStore } from '../store/uiStore'

// ── ConfigurationPage — /config/:section? ────────────────────
//
// Route-lazy Config section: three URL-synced tabs (project / pipeline /
// job). This page (and everything under components/config/) is the ONLY
// entry into lib/yaml-doc + lib/config-model, so the yaml stack stays in
// this lazy chunk — never import it from eager modules.
//
// All three tabs carry their structured editors: the lhp.yaml form
// (project/), the pipeline_config editor (pipeline/), and the
// job_config / monitoring_job_config editor (job/).
//
// Unsaved-changes guard: each tab lifts its useConfigFile dirty flag up
// here. Tab switches are intercepted at the Tabs' onValueChange (confirm
// before navigate); leaving /config entirely is blocked by the app-level
// NavigationGuard via a dirty-guard source (config-internal navs are
// exempted so the tab interception never double-prompts); hard refresh /
// window close raises the native beforeunload prompt via
// useBeforeUnloadGuard.

const SECTIONS = ['project', 'pipeline', 'job'] as const
type Section = (typeof SECTIONS)[number]

function isSection(value: string | undefined): value is Section {
  return (SECTIONS as readonly string[]).includes(value ?? '')
}

/** Report `dirty` upward; resets to clean when the reporting tab unmounts. */
function useDirtyLift(dirty: boolean, onDirtyChange: (dirty: boolean) => void): void {
  useEffect(() => {
    onDirtyChange(dirty)
  }, [dirty, onDirtyChange])
  useEffect(() => () => onDirtyChange(false), [onDirtyChange])
}

function ProjectTab({ onDirtyChange }: { onDirtyChange: (dirty: boolean) => void }) {
  const file = useConfigFile('lhp.yaml')
  useDirtyLift(file.dirty, onDirtyChange)
  return <ProjectConfigForm file={file} />
}

function FileConfigTab({
  kind,
  onDirtyChange,
}: {
  kind: ConfigTabKind
  onDirtyChange: (dirty: boolean) => void
}) {
  const stored = useUIStore((s) => s.selectedConfigFiles[kind])
  const setSelected = useUIStore((s) => s.setSelectedConfigFile)
  const { data: tree } = useFileList()
  const [createOpen, setCreateOpen] = useState(false)
  // File switch requested while the current form is dirty (confirm first).
  const [pendingFile, setPendingFile] = useState<string | null>(null)

  const available = useMemo(() => listConfigYamlFiles(tree, kind), [tree, kind])
  // While the tree is loading, trust the persisted selection (no flash);
  // once loaded, a selection that vanished from disk counts as none —
  // but stays in the store in case the file comes back (branch switch).
  const selected =
    stored !== null && (tree === undefined || available.includes(stored)) ? stored : null

  const file = useConfigFile(selected)
  useDirtyLift(file.dirty, onDirtyChange)
  const { title, description } = CONFIG_TAB_COPY[kind]

  // Switching files replaces the parsed handle wholesale, so a dirty form
  // must confirm the discard first (picker select AND create-from-template).
  const requestSelect = (path: string) => {
    if (path === selected) return
    if (file.dirty) setPendingFile(path)
    else setSelected(kind, path)
  }

  const picker = <ConfigFilePicker kind={kind} value={selected} onSelect={requestSelect} />
  const actions = (
    <Button type="button" variant="outline" size="sm" onClick={() => setCreateOpen(true)}>
      <FilePlus2 aria-hidden="true" />
      New from template
    </Button>
  )
  const dialog = (
    <>
      <CreateFromTemplateDialog
        kind={kind}
        open={createOpen}
        onOpenChange={setCreateOpen}
        onCreated={requestSelect}
      />
      <DiscardChangesDialog
        open={pendingFile !== null}
        onOpenChange={(open) => {
          if (!open) setPendingFile(null)
        }}
        description="This file has unsaved changes. Switching files discards them."
        onDiscard={() => {
          const next = pendingFile
          setPendingFile(null)
          if (next !== null) setSelected(kind, next)
        }}
      />
    </>
  )

  if (selected === null) {
    return (
      <>
        <ConfigPageShell title={title} description={description} picker={picker} actions={actions}>
          <EmptyState
            title="No config file selected"
            message="Pick a YAML file under config/, or create one from the packaged template."
            icon={FileCog}
            action={{
              label: 'Create from template',
              onClick: () => setCreateOpen(true),
              variant: 'outline',
            }}
          />
        </ConfigPageShell>
        {dialog}
      </>
    )
  }

  return (
    <>
      {kind === 'pipeline' ? (
        <PipelineConfigEditor file={file} picker={picker} actions={actions} />
      ) : (
        <JobConfigEditor file={file} picker={picker} actions={actions} />
      )}
      {dialog}
    </>
  )
}

export function ConfigurationPage() {
  const { section } = useParams()
  const navigate = useNavigate()
  const [dirty, setDirty] = useState(false)
  const [pendingSection, setPendingSection] = useState<Section | null>(null)
  // Hard refresh / window close while dirty → native browser prompt.
  useBeforeUnloadGuard(dirty)

  // GUARD INVARIANT — exactly one prompt per navigation attempt:
  //  • Tab switches are confirmed by the page's OWN dialog (the Tabs
  //    onValueChange interception below). On confirm it writes the target
  //    into `approvedNavRef` and navigates, so the app-level
  //    NavigationGuard (which is still registered at that instant — the
  //    dirty flag clears one commit later) lets exactly that navigation
  //    through without a second prompt.
  //  • EVERY other navigation while dirty — leaving /config or jumping
  //    into a /config/<section> from anywhere else — is blocked by the
  //    NavigationGuard through this source. Do not add links/navigations
  //    into config sections that bypass `requestSection`; they will (by
  //    design) surface the NavigationGuard prompt instead.
  // An approval is single-use: it is cleared as soon as the route or the
  // dirty flag changes, so a stale approval can never exempt a later nav.
  const approvedNavRef = useRef<string | null>(null)
  useEffect(() => {
    approvedNavRef.current = null
  }, [section, dirty])
  useDirtyGuardSource(
    'config-form',
    useMemo(
      () =>
        dirty
          ? {
              message:
                'The configuration form has unsaved changes. Leaving this page discards them.',
              blocks: (nextPathname: string) => nextPathname !== approvedNavRef.current,
              onDiscard: () => setDirty(false),
            }
          : null,
      [dirty],
    ),
  )

  // Normalize /config and /config/<anything-unknown> to /config/project.
  if (!isSection(section)) return <Navigate to="/config/project" replace />

  const requestSection = (value: string) => {
    const next = value as Section
    if (next === section) return
    if (dirty) setPendingSection(next)
    else void navigate(`/config/${next}`)
  }

  return (
    <div className="flex h-full min-h-0 flex-col bg-background">
      <div className="border-b border-border px-6 pt-4">
        <h1 className="text-lg font-semibold text-foreground">Configuration</h1>
        <p className="mt-1 text-xs text-muted-foreground">
          Project, pipeline and job settings — edited in place, comments preserved.
        </p>
        <Tabs value={section} onValueChange={requestSection} className="mt-3">
          <TabsList variant="line" aria-label="Configuration surfaces">
            <TabsTrigger value="project">Project</TabsTrigger>
            <TabsTrigger value="pipeline">Pipelines</TabsTrigger>
            <TabsTrigger value="job">Jobs</TabsTrigger>
          </TabsList>
        </Tabs>
      </div>

      {section === 'project' && <ProjectTab onDirtyChange={setDirty} />}
      {section === 'pipeline' && <FileConfigTab kind="pipeline" onDirtyChange={setDirty} />}
      {section === 'job' && <FileConfigTab kind="job" onDirtyChange={setDirty} />}

      {/* Tab-switch confirmation (works regardless of the route blocker). */}
      <DiscardChangesDialog
        open={pendingSection !== null}
        onOpenChange={(open) => {
          if (!open) setPendingSection(null)
        }}
        description="This configuration tab has unsaved changes. Switching tabs discards them."
        onDiscard={() => {
          const next = pendingSection
          setPendingSection(null)
          setDirty(false)
          if (next !== null) {
            // Single-use pass for the route blocker (see GUARD INVARIANT).
            approvedNavRef.current = `/config/${next}`
            void navigate(`/config/${next}`)
          }
        }}
      />
    </div>
  )
}
