import { useMemo, useState } from 'react'
import type { ReactNode } from 'react'
import { TriangleAlert } from 'lucide-react'
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from '@/components/ui/alert-dialog'
import { Badge } from '@/components/ui/badge'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { EmptyState } from '../../common/EmptyState'
import { SkeletonLoader } from '../../common/SkeletonLoader'
import {
  classifyPipelineDoc,
  isPlainObject,
  listPipelinePassthroughKeys,
  PIPELINE_BUILTIN_DEFAULTS,
  validatePipelineConfigFile,
} from '../../../lib/config-model'
import { addDocument, removeDocument } from '../../../lib/yaml-doc'
import { ParseErrorsCard } from '../ParseErrorsCard'
import { PipelineDocForm } from './PipelineDocForm'
import { PipelineDocList } from './PipelineDocList'
import { UseForRunsToggle } from './UseForRunsToggle'
import { bindDocApi, issuesAtExactly, snapshotDocs } from '../shared/docFormSupport'
import type { ConfigDocSource, RailSelection } from '../shared/docFormSupport'
import { buildRailDocs, duplicateNames } from './pipelineFormSupport'

// ── PipelineConfigEditor — the pipeline_config editor body ───
//
// Master-detail over one config document source: the precedence rail
// (PipelineDocList) on the left, the selected document's form on the
// right. Validation runs file-wide (validatePipelineConfigFile) so the
// duplicate matrix (VAL_006) badges every involved document; issues render
// per field but no longer block anything (saving is the buffer's ⌘S path).
// Re-hosted by components/entity/ConfigFormView, which owns the scroll
// frame, degraded banner, and viewer read-only wrapper.
//
// DELIBERATE OMISSION — VAL_010: the `__eventlog_monitoring` alias
// clashing with the ACTUAL monitoring pipeline name is NOT mirrored
// client-side. Resolving the alias requires lhp.yaml's monitoring
// configuration, which lives outside this file; mirroring it here would
// mean cross-file validation state the form doesn't own. The CLI loader
// (`_resolve_monitoring_alias`) still catches it on validate/generate.

export interface PipelineConfigEditorProps {
  /** The pipeline_config document source (documentStore-backed). */
  file: ConfigDocSource
}

/** Read-only card for the rail's ghost row. */
function BuiltinDefaultsCard() {
  return (
    <Card className="gap-3 py-4" data-testid="builtin-defaults-card">
      <CardHeader className="px-4">
        <CardTitle className="text-xs">Built-in defaults</CardTitle>
        <CardDescription className="text-2xs">
          Built into LHP — the lowest merge layer. A value here applies whenever
          neither project defaults nor a pipeline's own document sets the key.
        </CardDescription>
      </CardHeader>
      <CardContent className="px-4">
        <dl className="space-y-1">
          {Object.entries(PIPELINE_BUILTIN_DEFAULTS).map(([key, value]) => (
            <div key={key} className="flex items-center gap-3">
              <dt className="w-32 shrink-0 font-mono text-xs text-muted-foreground">{key}</dt>
              <dd className="font-mono text-xs">{String(value)}</dd>
            </div>
          ))}
        </dl>
        <p className="mt-2 text-2xs text-muted-foreground">
          packaging additionally defaults to "source" (applied at resolve time).
        </p>
      </CardContent>
    </Card>
  )
}

/** Passthrough-only card for documents the loader ignores. */
function UnrecognizedDocCard({ doc }: { doc: unknown }) {
  const keys = isPlainObject(doc) ? listPipelinePassthroughKeys(doc) : []
  return (
    <Card className="gap-3 py-4">
      <CardHeader className="px-4">
        <CardTitle className="text-xs">Unrecognized document</CardTitle>
        <CardDescription className="text-2xs">
          This document is ignored by LHP — it has neither project_defaults nor
          pipeline. Its content is kept exactly as written on save.
        </CardDescription>
      </CardHeader>
      {keys.length > 0 && (
        <CardContent className="flex flex-wrap gap-1.5 px-4">
          {keys.map((key) => (
            <Badge
              key={key}
              variant="outline"
              className="rounded-sm px-1.5 font-mono text-2xs font-normal text-muted-foreground"
            >
              {key}
            </Badge>
          ))}
        </CardContent>
      )}
    </Card>
  )
}

export function PipelineConfigEditor({ file }: PipelineConfigEditorProps) {
  const parsed = file.handle !== null && file.errors.length === 0

  // The handle is mutable: derive ONLY from [handle, version] (hook contract).
  const docs = useMemo(
    () => snapshotDocs(file),
    // eslint-disable-next-line react-hooks/exhaustive-deps -- version IS the handle's change signal
    [file.handle, file.version, parsed],
  )
  const issues = useMemo(() => (parsed ? validatePipelineConfigFile(docs) : []), [docs, parsed])
  const duplicates = useMemo(() => duplicateNames(docs), [docs])
  const rail = useMemo(() => buildRailDocs(docs, issues), [docs, issues])

  // Selection: null = "not chosen yet" → the per-file default is DERIVED
  // at render time (defaults doc, else first doc, else the built-ins
  // row). Deriving instead of initializing via an effect means the form
  // appears in the same commit as the rail — no intermediate state.
  const [chosen, setChosen] = useState<RailSelection | null>(null)
  const [focusMembership, setFocusMembership] = useState(false)
  const [pendingDelete, setPendingDelete] = useState<number | null>(null)

  // Reset the choice when the hook switches files (render-phase state
  // adjustment — no effect, no extra commit).
  const [pathFor, setPathFor] = useState(file.path)
  if (pathFor !== file.path) {
    setPathFor(file.path)
    setChosen(null)
    setFocusMembership(false)
  }

  const defaultsIndex = docs.findIndex((doc) => classifyPipelineDoc(doc) === 'defaults')
  const selected: RailSelection =
    chosen ?? (defaultsIndex >= 0 ? defaultsIndex : docs.length > 0 ? 0 : 'builtin')

  // Clamp a selection that outlived its document (external reload shrank the file).
  const activeSelection: RailSelection =
    typeof selected === 'number' && selected >= docs.length ? 'builtin' : selected

  const setSelected = (next: RailSelection) => setChosen(next)

  const select = (next: RailSelection) => {
    setFocusMembership(false)
    setSelected(next)
  }

  const addDoc = (initial: unknown, focusGroup = false) => {
    let index = -1
    file.mutate((handle) => {
      index = addDocument(handle, initial)
    })
    setSelected(index)
    setFocusMembership(focusGroup)
  }

  const confirmDelete = () => {
    const index = pendingDelete
    setPendingDelete(null)
    if (index === null) return
    file.mutate((handle) => removeDocument(handle, index))
    const remaining = docs.length - 1
    setSelected(remaining > 0 ? Math.min(index, remaining - 1) : 'builtin')
    setFocusMembership(false)
  }

  let body: ReactNode
  if (file.isLoading) {
    body = <SkeletonLoader lines={6} />
  } else if (file.loadError !== null) {
    body = <EmptyState title="Failed to load file" message={file.loadError} icon={TriangleAlert} />
  } else if (file.handle === null) {
    body = null
  } else if (file.errors.length > 0) {
    body = <ParseErrorsCard errors={file.errors} />
  } else {
    const activeDoc = typeof activeSelection === 'number' ? docs[activeSelection] : undefined
    const activeKind =
      typeof activeSelection === 'number' ? classifyPipelineDoc(activeDoc) : 'builtin'

    let detail: ReactNode
    if (activeSelection === 'builtin') {
      detail = <BuiltinDefaultsCard />
    } else if (activeKind === 'unrecognized') {
      detail = (
        <>
          {issuesAtExactly(issues, activeSelection, []).map((issue, i) => (
            <p key={i} role="alert" className="text-2xs text-warning">
              {issue.message}
            </p>
          ))}
          <UnrecognizedDocCard doc={activeDoc} />
        </>
      )
    } else {
      const kind = activeKind === 'defaults' ? 'defaults' : 'pipeline'
      const base = kind === 'defaults' ? ['project_defaults'] : []
      const docMap = isPlainObject(activeDoc) ? activeDoc : {}
      const settingsRaw = kind === 'defaults' ? docMap.project_defaults : docMap
      const settings = isPlainObject(settingsRaw)
        ? kind === 'defaults'
          ? settingsRaw
          : Object.fromEntries(Object.entries(settingsRaw).filter(([key]) => key !== 'pipeline'))
        : {}
      const docScopeIssues = [
        ...issuesAtExactly(issues, activeSelection, []),
        ...(kind === 'defaults' ? issuesAtExactly(issues, activeSelection, base) : []),
      ]
      detail = (
        <PipelineDocForm
          api={bindDocApi(file, activeSelection, base, settings, issues)}
          kind={kind}
          docSnapshot={docMap}
          duplicates={duplicates}
          docScopeIssues={docScopeIssues}
          onDelete={() => setPendingDelete(activeSelection)}
          focusMembership={focusMembership}
        />
      )
    }

    body = (
      <>
        <div className="flex gap-5">
          <PipelineDocList
            rail={rail}
            selected={activeSelection}
            onSelect={select}
            canEdit={parsed}
            onAddSingle={() => addDoc({ pipeline: 'new_pipeline' })}
            onAddGroup={() => addDoc({ pipeline: [] }, true)}
            onAddDefaults={() => addDoc({ project_defaults: {} })}
          />
          <div className="min-w-0 flex-1 space-y-4">{detail}</div>
        </div>
      </>
    )
  }

  return (
    <>
      {file.path !== null && (
        <div className="flex justify-end">
          <UseForRunsToggle path={file.path} />
        </div>
      )}
      {body}
      <AlertDialog
        open={pendingDelete !== null}
        onOpenChange={(open) => {
          if (!open) setPendingDelete(null)
        }}
      >
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle className="text-base">Delete this document?</AlertDialogTitle>
            <AlertDialogDescription className="text-xs">
              Removes the whole YAML document from the file (its comments go with
              it). Other documents keep their exact bytes.
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel size="sm">Keep document</AlertDialogCancel>
            <AlertDialogAction variant="destructive" size="sm" onClick={confirmDelete}>
              Delete
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </>
  )
}
