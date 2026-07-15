import { useCallback, useReducer, useRef, useState, type ReactNode } from 'react'
import { useQueryClient } from '@tanstack/react-query'
import { toast } from 'sonner'
import { ChevronDown, ChevronRight, TriangleAlert } from 'lucide-react'
import { cn } from '@/lib/utils'
import {
  deleteActionField,
  listActions,
  parseFlowgroupFile,
  selectFlowgroupAt,
  selectTemplate,
  setActionField,
} from '@/lib/flowgroup-doc'
import type { ActionRead, FlowgroupDocHandle, FlowgroupFileHandle } from '@/lib/flowgroup-doc'
import { useWorkspaceStore } from '@/store/workspaceStore'
import type { DocKind } from '@/store/workspaceStore'
import { useRunController } from '@/store/runStore'
import { persistBufferToDisk } from '@/workspace/persistBuffer'
import { useFlowgroupDoc } from '@/components/entity/useFlowgroupDoc'
import { SchemaKindProvider } from '@/components/common/SchemaKindContext'
import { OptionalTextField } from '@/components/config/fields/OptionalTextField'
import { Button } from '@/components/ui/button'
import { SegmentedControl } from '@/components/ui/segmented-control'
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
import { getActionSpec, listActionSpecs } from './specs/registry'
import type { FieldGroup } from './specs/types'
import { buildActionSkeleton } from './actionSkeleton'
import {
  computeIssues,
  fieldVisible,
  groupHasValueOrIssue,
  pathKey,
  visibleGroups,
  type DesignerMutator,
} from './formModel'
import { FieldRenderer } from './FieldRenderer'
import { MetadataMultiSelect } from './MetadataMultiSelect'
import { CodeModal, type CodeTarget } from './CodeModal'

// ── ActionModalEditor — the staged-save action editor (Task 3.1b) ─
//
// The modal BODY that replaces the graph's flattener (Task 3.2 mounts it inside
// a Dialog). Edits are STAGED on a DETACHED FlowgroupDocHandle parsed from the
// current buffer text — nothing touches the shared documentStore/workspace
// buffer until Save. Each field edit is recorded as a `DesignerMutator` closure
// (record-and-replay): it mutates the working handle for live rendering AND is
// pushed onto an ordered list. Save replays the recorded closures against the
// REAL handle in ONE atomic `commit`, then persists to disk; Cancel drops the
// working handle + the recorded list with nothing to roll back.
//
// The name is a READ-ONLY display: a staged rename would change the action name
// in the working handle, after which subsequent mutators keyed by the original
// `actionId` (name-scan resolution) would fail — and the props carry no rename
// callback. Renaming stays a graph-node concern (deliberate v1 scope).
//
// The header sub-type IS an interactive discriminator (Task 3.1c): a
// SegmentedControl of the kind's sub-types. Switching it is DESTRUCTIVE — after
// a confirm dialog, every non-preserved top-level key is dropped and the new
// sub-type's skeleton is seeded, all as STAGED mutators (so it too replays on
// Save and discards on Cancel). name/target/description/operational_metadata are
// kept. The current sub-type is local state (`action.subType` seeds it but goes
// stale after a switch), and the spec/sections re-resolve from that state.

/** Kind accent rail for the header. */
const KIND_RAIL: Record<string, string> = {
  load: 'bg-kind-load',
  transform: 'bg-kind-transform',
  write: 'bg-kind-write',
  test: 'bg-kind-test',
}

export interface ActionModalEditorProps {
  /** The entity tab's flowgroup/template file. */
  filePath: string
  docKind: DocKind
  /** The selected action (carries `.raw`, `.kind`, `.subType`, `.index`, …). */
  action: ActionRead
  /** Mutator address (canvas node id — valid even for duplicate/unnamed names). */
  actionId: string
  /** Called only after a Save that both replayed AND persisted (host closes). */
  onSaved?: () => void
  /** Discard + close (host closes the Dialog); never writes. */
  onCancel?: () => void
  /** Escape hatch for a failed persist (412 / yaml_error): host closes + jumps
   * to the Code view where the full conflict / syntax fix lives. */
  onOpenCodeView?: () => void
}

/** Select the one editable flowgroup/template from a freshly parsed file
 * (mirrors useFlowgroupDoc's private selectEntity). */
function selectWorkingDoc(
  file: FlowgroupFileHandle,
  docKind: DocKind,
): FlowgroupDocHandle | null {
  return docKind === 'template'
    ? (selectTemplate(file)?.body ?? null)
    : (selectFlowgroupAt(file, 0) ?? null)
}

export function ActionModalEditor({
  filePath,
  docKind,
  action,
  actionId,
  onSaved,
  onCancel,
  onOpenCodeView,
}: ActionModalEditorProps) {
  const { commit, readOnly } = useFlowgroupDoc(filePath, docKind)
  const queryClient = useQueryClient()
  const runController = useRunController()

  // The detached working handle + the ordered recorded mutators. Rebuilt only
  // when filePath/actionId change (Task 3.2 also keys the component per action,
  // so this is belt-and-braces). Reading the store here is a plain snapshot —
  // no subscription — so it does not re-run on unrelated buffer churn.
  const recorded = useRef<DesignerMutator[]>([])
  const built = useRef<{ key: string; doc: FlowgroupDocHandle | null }>({ key: '', doc: null })
  const buildKey = `${filePath}::${actionId}`
  if (built.current.key !== buildKey) {
    const text = useWorkspaceStore.getState().buffers.find((b) => b.path === filePath)?.content ?? ''
    built.current = { key: buildKey, doc: selectWorkingDoc(parseFlowgroupFile(text), docKind) }
    recorded.current = []
  }
  const workingDoc = built.current.doc

  const [, bump] = useReducer((n: number) => n + 1, 0)
  const [saving, setSaving] = useState(false)
  const [saveFailed, setSaveFailed] = useState(false)
  const [codeTarget, setCodeTarget] = useState<CodeTarget | null>(null)
  // The LIVE sub-type. `action.subType` seeds it but goes stale after a switch,
  // so the spec/sections/metadata resolve from THIS, not the prop. `pendingSubType`
  // holds a not-yet-confirmed switch target (drives the confirm dialog).
  const [currentSubType, setCurrentSubType] = useState(action.subType)
  const [pendingSubType, setPendingSubType] = useState<string | null>(null)

  // The `commit` every field writes through: apply the mutator to the working
  // handle for live rendering, record it for replay, and force a re-render. The
  // closures must close over actionId/path/value (NOT `workingDoc`) so they
  // replay against the real doc at Save.
  const stagedCommit = useCallback(
    (mutator: DesignerMutator) => {
      if (workingDoc === null) return
      setSaveFailed(false)
      mutator(workingDoc)
      recorded.current.push(mutator)
      bump()
    },
    [workingDoc],
  )

  const save = useCallback(async () => {
    if (recorded.current.length === 0 || readOnly) return
    setSaveFailed(false)
    const mutators = recorded.current.slice()
    // Replay the whole staged batch in ONE atomic commit against the real,
    // comment-preserving handle (documentStore.mutate re-anchors + returns
    // false on a partway throw, same as any single-field edit).
    const ok = commit((realDoc) => {
      for (const mutator of mutators) mutator(realDoc)
    })
    if (!ok) {
      toast.error('Could not save — the document is read-only or has parse errors.')
      return
    }
    setSaving(true)
    try {
      const persisted = await persistBufferToDisk(filePath, queryClient, runController)
      if (persisted) onSaved?.()
      // A false persist (412 / yaml_error) already toasted the specifics; flip
      // the in-modal recovery banner so Save is not a dead-end.
      else setSaveFailed(true)
    } finally {
      setSaving(false)
    }
  }, [readOnly, commit, filePath, queryClient, runController, onSaved])

  const onEditCode = useCallback((target: CodeTarget) => setCodeTarget(target), [])
  // A file-ref's "Open as file tab" hands the file to the docked workspace
  // editor; inline code routes back through stagedCommit (below).
  const openAsFile = useCallback((path: string) => {
    useWorkspaceStore.getState().openBuffer(path)
    setCodeTarget(null)
  }, [])

  const spec = getActionSpec(action.kind, currentSubType)
  const workingRaw = workingDoc ? listActions(workingDoc)[action.index]?.raw : undefined
  const dirty = recorded.current.length > 0

  // The kind's registered sub-types → segmented options (value=subType, label=title).
  const subTypeOptions = listActionSpecs()
    .filter((s) => s.kind === action.kind)
    .map((s) => ({ value: s.subType, label: s.title, disabled: readOnly }))
  const pendingTitle =
    pendingSubType === null ? '' : (getActionSpec(action.kind, pendingSubType)?.title ?? pendingSubType)

  // A change gets confirmed first (Decision 8) — never mutate on the raw click.
  const requestSubTypeChange = (next: string) => {
    if (next !== currentSubType) setPendingSubType(next)
  }

  // Confirmed switch: DESTRUCTIVE reset, all through stagedCommit so it stages
  // (records for Save-replay, discards on Cancel) exactly like every field edit.
  // Drop every non-preserved top-level key currently present, then seed the new
  // sub-type's skeleton (its discriminator + required fields); name/target/
  // description/operational_metadata are never touched.
  const confirmSubTypeChange = () => {
    const next = pendingSubType
    setPendingSubType(null)
    const kind = action.kind
    if (next === null || kind === undefined || workingRaw === undefined || readOnly) return

    const PRESERVED = ['name', 'target', 'description', 'operational_metadata']
    for (const key of Object.keys(workingRaw)) {
      if (!PRESERVED.includes(key)) {
        stagedCommit((doc) => deleteActionField(doc, actionId, [key]))
      }
    }
    const existingNames = workingDoc ? listActions(workingDoc).map((a) => a.name) : []
    const skeleton = buildActionSkeleton(kind, next, existingNames)
    for (const key of Object.keys(skeleton)) {
      if (!PRESERVED.includes(key)) {
        const value = skeleton[key]
        stagedCommit((doc) => setActionField(doc, actionId, [key], value))
      }
    }
    setCurrentSubType(next)
  }

  const header = (
    <div className="border-b border-border px-4 pb-3 pt-4">
      <div className={cn('mb-2 h-0.5 w-8 rounded-full', KIND_RAIL[action.kind ?? ''] ?? 'bg-border')} />
      <div className="truncate font-mono text-sm font-semibold text-foreground" title={action.name}>
        {action.name === '' ? '(unnamed)' : action.name}
      </div>
      {subTypeOptions.length > 0 && (
        <div className="mt-2">
          <SegmentedControl
            size="sm"
            aria-label="Action sub-type"
            value={currentSubType}
            onValueChange={requestSubTypeChange}
            options={subTypeOptions}
          />
        </div>
      )}
      {spec?.summary && <div className="mt-1.5 text-2xs text-muted-foreground">{spec.summary}</div>}
    </div>
  )

  const footer = (
    <div className="mt-3 flex items-center gap-2 border-t border-border px-4 py-2.5">
      <Button type="button" variant="ghost" size="xs" onClick={() => onCancel?.()}>
        Cancel
      </Button>
      <Button
        type="button"
        size="xs"
        disabled={!dirty || saving || readOnly}
        onClick={() => void save()}
      >
        Save
      </Button>
      {readOnly ? (
        <span className="ml-auto text-2xs text-muted-foreground">Read-only</span>
      ) : (
        dirty && (
          <span className="ml-auto flex items-center gap-1.5 text-2xs text-faint">
            <span className="size-1.5 rounded-full bg-warning" aria-hidden="true" />
            Editing action · unsaved
          </span>
        )
      )}
    </div>
  )

  const banner = saveFailed && (
    <div
      role="alert"
      className="mx-4 mt-3 flex items-start gap-2 rounded-sm border border-warning/30 bg-warning/10 px-2.5 py-2 text-2xs text-foreground"
    >
      <TriangleAlert className="mt-0.5 size-3.5 shrink-0 text-warning" aria-hidden="true" />
      <span className="flex-1">
        Couldn&apos;t save to disk — the file changed on disk or has a syntax error. Resolve it in
        the Code view.
      </span>
      <Button type="button" variant="outline" size="xs" onClick={() => onOpenCodeView?.()}>
        Open in Code view
      </Button>
    </div>
  )

  const codeModal = (
    <CodeModal
      target={codeTarget}
      readOnly={readOnly}
      commit={stagedCommit}
      openAsFile={openAsFile}
      onClose={() => setCodeTarget(null)}
    />
  )

  const confirmDialog = (
    <AlertDialog
      open={pendingSubType !== null}
      onOpenChange={(open) => {
        if (!open) setPendingSubType(null)
      }}
    >
      <AlertDialogContent>
        <AlertDialogHeader>
          <AlertDialogTitle>Switch to {pendingTitle}?</AlertDialogTitle>
          <AlertDialogDescription>
            This resets the action&apos;s configuration. Name, target, description, and operational
            metadata are kept.
          </AlertDialogDescription>
        </AlertDialogHeader>
        <AlertDialogFooter>
          <AlertDialogCancel>Cancel</AlertDialogCancel>
          <AlertDialogAction variant="destructive" onClick={confirmSubTypeChange}>
            Switch
          </AlertDialogAction>
        </AlertDialogFooter>
      </AlertDialogContent>
    </AlertDialog>
  )

  // No form for this sub-type (unknown discriminator) or the action fell out of
  // the working parse: point at the Code view rather than dead-end.
  if (spec === undefined || workingRaw === undefined) {
    return (
      <div className="flex flex-col">
        {header}
        <div className="px-4 py-4">
          <p className="text-xs text-muted-foreground">
            No form for this action type — edit it in the Code view.
          </p>
          <Button
            type="button"
            variant="outline"
            size="xs"
            className="mt-2"
            onClick={() => onOpenCodeView?.()}
          >
            Open in Code view
          </Button>
        </div>
        {footer}
        {codeModal}
        {confirmDialog}
      </div>
    )
  }

  const issues = computeIssues(spec, workingRaw)
  const groups = visibleGroups(spec, workingRaw)

  return (
    <div className="flex flex-col">
      {header}

      <SchemaKindProvider kind="flowgroup">
        <div className="flex flex-col gap-4 px-4 py-4">
          <OptionalTextField
            id={`ame-${actionId}-description`}
            label="Description"
            value={workingRaw.description}
            onSet={(value) =>
              stagedCommit((doc) => setActionField(doc, actionId, ['description'], value))
            }
            onUnset={() => stagedCommit((doc) => deleteActionField(doc, actionId, ['description']))}
            disabled={readOnly}
          />

          {groups.map((group, gi) => {
            const fields = group.fields.filter((field) => fieldVisible(field, workingRaw))
            if (fields.length === 0) return null
            // Non-advanced groups are always open (required-to-generate fields
            // must never hide behind a collapsed section); an advanced/collapsed
            // group opens only when it already carries a value or an issue.
            const defaultOpen =
              !(group.advanced || group.collapsed) || groupHasValueOrIssue(group, workingRaw)
            return (
              <CollapsibleGroup key={group.title ?? gi} group={group} defaultOpen={defaultOpen}>
                {fields.map((field) => (
                  <FieldRenderer
                    key={pathKey(field.path)}
                    field={field}
                    raw={workingRaw}
                    actionId={actionId}
                    commit={stagedCommit}
                    issue={issues.get(pathKey(field.path))}
                    disabled={readOnly}
                    saving={saving}
                    onEditCode={onEditCode}
                    tokenComplete
                  />
                ))}
              </CollapsibleGroup>
            )
          })}

          <MetadataSection
            kind={action.kind}
            value={workingRaw.operational_metadata}
            onChange={(next) =>
              stagedCommit((doc) => setActionField(doc, actionId, ['operational_metadata'], next))
            }
          />
        </div>
      </SchemaKindProvider>

      {banner}
      {footer}
      {codeModal}
      {confirmDialog}
    </div>
  )
}

/** A spec group as a disclosure. A titled group renders a toggle header; a
 * title-less group is always shown. Initial open state is decided once at
 * mount (see caller) and never force-collapsed on later edits. */
function CollapsibleGroup({
  group,
  defaultOpen,
  children,
}: {
  group: FieldGroup
  defaultOpen: boolean
  children: ReactNode
}) {
  const [open, setOpen] = useState(defaultOpen)

  if (group.title === undefined) {
    return <div className="flex flex-col gap-3">{children}</div>
  }

  return (
    <section className="flex flex-col gap-3 border-t border-border pt-3">
      <button
        type="button"
        aria-expanded={open}
        onClick={() => setOpen((o) => !o)}
        className="flex items-center gap-1.5 text-left"
      >
        {open ? (
          <ChevronDown className="size-3 text-muted-foreground" aria-hidden="true" />
        ) : (
          <ChevronRight className="size-3 text-muted-foreground" aria-hidden="true" />
        )}
        <span className="text-2xs font-semibold uppercase tracking-[0.05em] text-muted-foreground">
          {group.title}
        </span>
      </button>
      {open && (
        <>
          {group.description && (
            <p className="-mt-1.5 text-2xs text-muted-foreground">{group.description}</p>
          )}
          <div className="flex flex-col gap-3">{children}</div>
        </>
      )}
    </section>
  )
}

/**
 * The shell-level operational-metadata surface (Decision 6): load/transform get
 * the interactive MetadataMultiSelect (both apply to a `view`); write gets an
 * informational note (the metadata is applied upstream and inherited); test has
 * no metadata section.
 */
function MetadataSection({
  kind,
  value,
  onChange,
}: {
  kind: ActionRead['kind']
  value: unknown
  onChange: (next: boolean | string[]) => void
}) {
  if (kind === 'load' || kind === 'transform') {
    return (
      <section className="flex flex-col gap-2 border-t border-border pt-3">
        <div className="text-2xs font-semibold uppercase tracking-[0.05em] text-muted-foreground">
          Operational metadata
        </div>
        <MetadataMultiSelect
          value={value as boolean | string[] | undefined}
          onChange={onChange}
          applies_to="view"
        />
      </section>
    )
  }
  if (kind === 'write') {
    return (
      <section className="flex flex-col gap-2 border-t border-border pt-3">
        <div className="text-2xs font-semibold uppercase tracking-[0.05em] text-muted-foreground">
          Operational metadata
        </div>
        <p className="text-2xs text-muted-foreground">
          Operational metadata is applied by the upstream load/transform actions that create these
          views, and inherited here.
        </p>
      </section>
    )
  }
  return null
}
