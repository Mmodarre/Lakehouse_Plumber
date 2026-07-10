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
import { EmptyState } from '../../common/EmptyState'
import { SkeletonLoader } from '../../common/SkeletonLoader'
import type { UseConfigFileResult } from '../../../hooks/useConfigFile'
import {
  classifyJobDoc,
  isPlainObject,
  listJobPassthroughKeys,
  validateJobConfigFile,
} from '../../../lib/config-model'
import { addDocument, removeDocument } from '../../../lib/yaml-doc'
import { ConfigConflictDialog } from '../ConfigConflictDialog'
import { ConfigPageShell } from '../ConfigPageShell'
import { CONFIG_TAB_COPY } from '../configFileSupport'
import { ExternalChangeBanner } from '../ExternalChangeBanner'
import { ParseErrorsCard } from '../ParseErrorsCard'
import { SaveBar } from '../SaveBar'
import {
  bindDocApi,
  countErrors,
  issuesAtExactly,
  snapshotDocs,
} from '../shared/docFormSupport'
import type { RailSelection } from '../shared/docFormSupport'
import { JobDocForm } from './JobDocForm'
import { JobDocList } from './JobDocList'
import {
  JobBuiltinDefaultsCard,
  MonitoringFormatCard,
  UnrecognizedJobDocCard,
} from './JobEditorCards'
import { LegacyConvertBanner } from './LegacyConvertBanner'
import {
  buildJobRailDocs,
  convertLegacyJobDoc,
  duplicateJobNames,
  flatPassthroughKeys,
  jobFileMode,
  presentDocs,
} from './jobFormSupport'

// ── JobConfigEditor — the job_config editor ──────────────────
//
// One editor, three file shapes (jobFileMode):
//   • STANDARD job_config: master-detail with the precedence rail
//     (JobDocList) — like the pipeline editor. Validation runs file-wide
//     (validateJobConfigFile) so the duplicate-job_name matrix (VAL_004)
//     badges every involved document and Save is blocked while any
//     blocking error exists anywhere in the file.
//   • LEGACY flat job_config (single doc, no project_defaults): the
//     convert banner + the flat mapping edited AS project defaults (which
//     is exactly how the loader reads it). Conversion is explicit-only —
//     it never happens on save.
//   • MONITORING job_config (monitoring_job_config* by name): a single
//     flat settings form — its loader mandates one document, no
//     project_defaults wrapper, no job_name, so there is no rail and no
//     add-job affordance. A multi-document monitoring file is a blocking
//     error (yaml.safe_load raises on it at generate time).

export interface JobConfigEditorProps {
  /** A useConfigFile(<job config path>) instance owned by the page. */
  file: UseConfigFileResult
  /** File picker slot (job tab). */
  picker?: ReactNode
  /** Header actions slot ("New from template"). */
  actions?: ReactNode
}

export function JobConfigEditor({ file, picker, actions }: JobConfigEditorProps) {
  const parsed = file.handle !== null && file.errors.length === 0

  // The handle is mutable: derive ONLY from [handle, version] (hook contract).
  const docs = useMemo(
    () => snapshotDocs(file),
    // eslint-disable-next-line react-hooks/exhaustive-deps -- version IS the handle's change signal
    [file.handle, file.version, parsed],
  )
  const issues = useMemo(() => (parsed ? validateJobConfigFile(docs) : []), [docs, parsed])
  const duplicates = useMemo(() => duplicateJobNames(docs), [docs])
  const rail = useMemo(() => buildJobRailDocs(docs, issues), [docs, issues])

  const mode = jobFileMode(file.path, docs)
  const present = presentDocs(docs)
  // RAW document count: yaml.safe_load raises on ANY second document — even
  // the null one a trailing `---` produces — so nulls must count here.
  const monitoringMultiDoc = mode === 'monitoring' && parsed && docs.length > 1
  // Monitoring files bypass JobConfigLoader entirely (yaml.safe_load: one
  // document, read flat, rendered verbatim), so validateJobConfigFile's
  // multi-doc errors (VAL_003/VAL_004, project_defaults shape) don't apply.
  // The only blocking errors are the monitoring loader's own raises: more
  // than one document, or a non-mapping document (monitoring_service.py).
  const monitoringErrors = monitoringMultiDoc
    ? 1
    : parsed && present.length === 1 && !isPlainObject(present[0]!.doc)
      ? 1
      : 0
  const errorCount =
    file.errors.length + (mode === 'monitoring' ? monitoringErrors : countErrors(issues))

  // Selection (standard mode): null = "not chosen yet" → the per-file
  // default is DERIVED at render time (defaults doc, else first doc, else
  // the built-ins row) — same commit as the rail, no intermediate state.
  const [chosen, setChosen] = useState<RailSelection | null>(null)
  const [pendingDelete, setPendingDelete] = useState<number | null>(null)

  // Reset the choice when the hook switches files (render-phase state
  // adjustment — no effect, no extra commit).
  const [pathFor, setPathFor] = useState(file.path)
  if (pathFor !== file.path) {
    setPathFor(file.path)
    setChosen(null)
  }

  const defaultsIndex = docs.findIndex(
    (doc) => classifyJobDoc(doc, present.length) === 'defaults',
  )
  const selected: RailSelection =
    chosen ?? (defaultsIndex >= 0 ? defaultsIndex : docs.length > 0 ? 0 : 'builtin')
  // Clamp a selection that outlived its document (external reload shrank the file).
  const activeSelection: RailSelection =
    typeof selected === 'number' && selected >= docs.length ? 'builtin' : selected

  const addDoc = (initial: unknown) => {
    let index = -1
    file.mutate((handle) => {
      index = addDocument(handle, initial)
    })
    setChosen(index)
  }

  const confirmDelete = () => {
    const index = pendingDelete
    setPendingDelete(null)
    if (index === null) return
    file.mutate((handle) => removeDocument(handle, index))
    const remaining = docs.length - 1
    setChosen(remaining > 0 ? Math.min(index, remaining - 1) : 'builtin')
  }

  /** Detail form for one document (shared by all three modes). */
  const docForm = (
    docIndex: number,
    variant: 'defaults' | 'job' | 'monitoring',
    options: { flat?: boolean; onDelete?: () => void } = {},
  ) => {
    const doc = docs[docIndex]
    const docMap = isPlainObject(doc) ? doc : {}
    // Flat forms (monitoring + legacy) edit the whole mapping; standard
    // defaults docs edit inside project_defaults; job docs edit the root
    // minus job_name.
    const base = options.flat ? [] : variant === 'defaults' ? ['project_defaults'] : []
    const settingsRaw = options.flat
      ? docMap
      : variant === 'defaults'
        ? docMap.project_defaults
        : docMap
    const settings = isPlainObject(settingsRaw)
      ? variant === 'job'
        ? Object.fromEntries(Object.entries(settingsRaw).filter(([key]) => key !== 'job_name'))
        : settingsRaw
      : {}
    const docScopeIssues = [
      ...issuesAtExactly(issues, docIndex, []),
      ...(base.length > 0 ? issuesAtExactly(issues, docIndex, base) : []),
    ]
    const passthroughKeys = options.flat
      ? flatPassthroughKeys(docMap)
      : listJobPassthroughKeys(docMap, present.length)
    return (
      <JobDocForm
        api={bindDocApi(file, docIndex, base, settings, issues)}
        variant={variant}
        docSnapshot={docMap}
        duplicates={duplicates}
        docScopeIssues={docScopeIssues}
        passthroughKeys={passthroughKeys}
        onDelete={options.onDelete}
      />
    )
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
  } else if (mode === 'monitoring') {
    const first = present[0]
    // Keys that misuse the multi-doc vocabulary inside the flat format.
    const misusedKeys =
      first !== undefined && isPlainObject(first.doc)
        ? ['project_defaults', 'job_name'].filter((key) => key in (first.doc as object))
        : []
    body = (
      <>
        {monitoringMultiDoc ? (
          <p role="alert" className="text-xs text-destructive">
            This monitoring job config has {docs.length} YAML documents — the monitoring
            loader requires exactly one, and generation fails on this file. Merge the documents
            via "Open raw YAML".
          </p>
        ) : first === undefined ? (
          <>
            <MonitoringFormatCard />
            <EmptyState
              title="Empty file"
              message="LHP's built-in defaults apply as-is. Add a settings document to override them."
              icon={TriangleAlert}
              action={{
                label: 'Add settings',
                onClick: () => addDoc({}),
                variant: 'outline',
              }}
            />
          </>
        ) : (
          <>
            <MonitoringFormatCard />
            {misusedKeys.map((key) => (
              <p key={key} role="alert" className="text-2xs text-warning">
                '{key}' has no meaning in a monitoring job config — the whole file is already
                the settings, so this key is rendered verbatim into the job resource.
              </p>
            ))}
            {docForm(first.index, 'monitoring', { flat: true })}
          </>
        )}
      </>
    )
  } else if (mode === 'legacy') {
    const first = present[0]!
    body = (
      <>
        <LegacyConvertBanner
          onConvert={() => file.mutate((handle) => convertLegacyJobDoc(handle, first.index))}
        />
        {docForm(first.index, 'defaults', { flat: true })}
      </>
    )
  } else {
    const activeDoc = typeof activeSelection === 'number' ? docs[activeSelection] : undefined
    const activeKind =
      typeof activeSelection === 'number'
        ? classifyJobDoc(activeDoc, present.length)
        : 'builtin'

    let detail: ReactNode
    if (activeSelection === 'builtin') {
      detail = <JobBuiltinDefaultsCard />
    } else if (activeKind === 'unrecognized' || activeKind === 'legacy-flat') {
      // 'legacy-flat' is unreachable here (single flat docs route to the
      // legacy mode above); kept in the branch so the types stay exact.
      detail = (
        <>
          {issuesAtExactly(issues, activeSelection, []).map((issue, i) => (
            <p key={i} role="alert" className="text-2xs text-warning">
              {issue.message}
            </p>
          ))}
          <UnrecognizedJobDocCard doc={activeDoc} />
        </>
      )
    } else {
      detail = docForm(activeSelection, activeKind === 'defaults' ? 'defaults' : 'job', {
        onDelete: () => setPendingDelete(activeSelection),
      })
    }

    body = (
      <div className="flex gap-5">
        <JobDocList
          rail={rail}
          selected={activeSelection}
          onSelect={setChosen}
          canEdit={parsed}
          onAddSingle={() => addDoc({ job_name: 'new_job' })}
          onAddGroup={() => addDoc({ job_name: [] })}
          onAddDefaults={() => addDoc({ project_defaults: {} })}
        />
        <div className="min-w-0 flex-1 space-y-4">{detail}</div>
      </div>
    )
  }

  return (
    <>
      <ConfigPageShell
        title={CONFIG_TAB_COPY.job.title}
        description={CONFIG_TAB_COPY.job.description}
        picker={picker}
        actions={actions}
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
        {file.yamlError && (
          <p role="alert" className="text-xs text-destructive">
            Saved with a YAML syntax error (line {file.yamlError.line}, column{' '}
            {file.yamlError.column}): {file.yamlError.message}
          </p>
        )}
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
