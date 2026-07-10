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
import type { UseConfigFileResult } from '../../../hooks/useConfigFile'
import {
  classifyPipelineDoc,
  isPlainObject,
  listPipelinePassthroughKeys,
  PIPELINE_BUILTIN_DEFAULTS,
  validatePipelineConfigFile,
} from '../../../lib/config-model'
import { addDocument, removeDocument } from '../../../lib/yaml-doc'
import { ConfigConflictDialog } from '../ConfigConflictDialog'
import { ConfigPageShell } from '../ConfigPageShell'
import { CONFIG_TAB_COPY } from '../configFileSupport'
import { ExternalChangeBanner } from '../ExternalChangeBanner'
import { ParseErrorsCard } from '../ParseErrorsCard'
import { SaveBar } from '../SaveBar'
import { PipelineDocForm } from './PipelineDocForm'
import { PipelineDocList } from './PipelineDocList'
import { UseForRunsToggle } from './UseForRunsToggle'
import {
  bindDocApi,
  countErrors,
  issuesAtExactly,
  snapshotDocs,
} from '../shared/docFormSupport'
import type { RailSelection } from '../shared/docFormSupport'
import { buildRailDocs, duplicateNames } from './pipelineFormSupport'

// ── PipelineConfigEditor — the pipeline_config editor ────────
//
// Master-detail over one useConfigFile instance: the precedence rail
// (PipelineDocList) on the left, the selected document's form on the
// right. Validation runs file-wide (validatePipelineConfigFile) so the
// duplicate matrix (VAL_006) badges every involved document and Save is
// blocked while any blocking error exists anywhere in the file.
//
// DELIBERATE OMISSION — VAL_010: the `__eventlog_monitoring` alias
// clashing with the ACTUAL monitoring pipeline name is NOT mirrored
// client-side. Resolving the alias requires lhp.yaml's monitoring
// configuration, which lives outside this file; mirroring it here would
// mean cross-file validation state the form doesn't own. The CLI loader
// (`_resolve_monitoring_alias`) still catches it on validate/generate.

export interface PipelineConfigEditorProps {
  /** A useConfigFile(<pipeline config path>) instance owned by the page. */
  file: UseConfigFileResult
  /** File picker slot (pipeline tab). */
  picker?: ReactNode
  /** Header actions slot ("New from template"). */
  actions?: ReactNode
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

export function PipelineConfigEditor({ file, picker, actions }: PipelineConfigEditorProps) {
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
  const errorCount = file.errors.length + countErrors(issues)

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
        {file.yamlError && (
          <p role="alert" className="text-xs text-destructive">
            Saved with a YAML syntax error (line {file.yamlError.line}, column{' '}
            {file.yamlError.column}): {file.yamlError.message}
          </p>
        )}
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
      <ConfigPageShell
        title={CONFIG_TAB_COPY.pipeline.title}
        description={CONFIG_TAB_COPY.pipeline.description}
        picker={picker}
        actions={
          <>
            {file.path !== null && <UseForRunsToggle path={file.path} />}
            {actions}
          </>
        }
        banner={
          file.externalChange ? (
            <ExternalChangeBanner onReload={() => void file.reload()} onKeep={file.keepMine} />
          ) : undefined
        }
        footer={
          <SaveBar
            path={file.path}
            dirty={file.dirty}
            saving={file.saving}
            errorCount={errorCount}
            onSave={() => void file.save()}
          />
        }
      >
        {body}
      </ConfigPageShell>
      <ConfigConflictDialog
        path={file.conflict ? file.path : null}
        saving={file.saving}
        onReload={() => void file.reload()}
        onOverwrite={() => void file.overwrite()}
        onCancel={file.dismissConflict}
      />
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
