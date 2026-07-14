import { useCallback, useMemo, useState } from 'react'
import { useQueryClient } from '@tanstack/react-query'
import { toast } from 'sonner'
import { TriangleAlert } from 'lucide-react'
import { cn } from '../../../lib/utils'
import { isPlainObject } from '../../../lib/config-model'
import { setActionField } from '../../../lib/flowgroup-doc'
import type { ActionRead } from '../../../lib/flowgroup-doc'
import type { DocKind } from '../../../store/workspaceStore'
import { useRunController } from '../../../store/runStore'
import { persistBufferToDisk } from '../../../workspace/persistBuffer'
import { useFlowgroupDoc } from '../../entity/useFlowgroupDoc'
import { KIND_STYLES, FALLBACK_KIND_STYLE } from '../../graph/nodes/kindStyles'
import { Button } from '../../ui/button'
import { Input } from '../../ui/input'
import { Textarea } from '../../ui/textarea'

// ── ActionEditor — inspector Action tab body (D5) ────────────────
//
// Edits the flowgroup graph's selected action. The selection (kind + name +
// mutator address `actionId`) arrives from selectionStore, published by
// GraphView; the write path is re-bound HERE via useFlowgroupDoc(filePath) —
// the SAME documentStore entry GraphView drives, so a Save mutates the shared,
// comment-preserving buffer and GraphView re-projects on the next version.
//
// Fields are derived generically from the action's raw mapping (one level of
// nested plain-objects flattened to dotted labels, e.g. `source.type`). Only
// STRING leaves are editable in v1; numbers / booleans / lists / deep objects
// render read-only (typed write-back for those is deferred — edit them in the
// Code view). Save applies every changed string leaf in ONE atomic commit.

/** A rendered field derived from the action's raw mapping. */
interface EditorField {
  /** Dotted label, e.g. `source.type`. */
  label: string
  /** yaml-doc address relative to the action. */
  path: (string | number)[]
  value: unknown
  /** String leaves are editable; everything else is read-only in v1. */
  editable: boolean
  multiline: boolean
}

const SKIP_TOP = new Set(['name', 'type'])
const MULTILINE_KEYS = new Set(['sql', 'select_expr', 'expression', 'code'])

function pathKey(path: (string | number)[]): string {
  return JSON.stringify(path)
}

function pushField(
  path: (string | number)[],
  label: string,
  value: unknown,
  out: EditorField[],
): void {
  if (typeof value === 'string') {
    const leaf = String(path[path.length - 1])
    out.push({
      label,
      path,
      value,
      editable: true,
      multiline: value.includes('\n') || MULTILINE_KEYS.has(leaf),
    })
    return
  }
  if (isPlainObject(value)) {
    for (const [k, v] of Object.entries(value)) pushField([...path, k], `${label}.${k}`, v, out)
    return
  }
  out.push({ label, path, value, editable: false, multiline: false })
}

/** Flatten an action's raw mapping into label/value rows (skips name + type). */
function flattenFields(raw: Record<string, unknown>): EditorField[] {
  const out: EditorField[] = []
  for (const [key, value] of Object.entries(raw)) {
    if (SKIP_TOP.has(key)) continue
    pushField([key], key, value, out)
  }
  return out
}

/** Read-only display of a non-string value. */
function DisplayValue({ value }: { value: unknown }) {
  if (Array.isArray(value)) {
    if (value.length === 0) return <span className="text-faint">[]</span>
    return (
      <span className="flex flex-wrap gap-1">
        {value.map((item, i) => (
          <span
            key={i}
            className="rounded-sm border border-border bg-background px-1.5 font-mono text-2xs text-muted-foreground"
          >
            {typeof item === 'object' ? JSON.stringify(item) : String(item)}
          </span>
        ))}
      </span>
    )
  }
  if (value === null || value === undefined) return <span className="text-faint">—</span>
  if (typeof value === 'boolean' || typeof value === 'number') {
    return <span className="text-syntax-number">{String(value)}</span>
  }
  return <span>{JSON.stringify(value)}</span>
}

export interface ActionEditorProps {
  action: ActionRead
  /** Mutator address (canvas node id ≡ action name / `name#N`). */
  actionId: string
  /** The entity tab's flowgroup/template file. */
  filePath: string
  docKind: DocKind
  /** Called after a Save that both committed AND persisted to disk (the modal
   * host uses it to close). Not called on a failed/degraded save. */
  onSaved?: () => void
  /** Escape hatch for a failed persist (412 / yaml_error): the host should
   * close the modal AND switch the entity tab to the Code view, where the full
   * ConflictDialog / syntax fix lives (Fix #2 — avoids a locked-Save dead-end). */
  onOpenCodeView?: () => void
}

export function ActionEditor({
  action,
  actionId,
  filePath,
  docKind,
  onSaved,
  onOpenCodeView,
}: ActionEditorProps) {
  const { commit, readOnly } = useFlowgroupDoc(filePath, docKind)
  const queryClient = useQueryClient()
  const runController = useRunController()

  const fields = useMemo(() => flattenFields(action.raw), [action.raw])
  const editableFields = useMemo(() => fields.filter((f) => f.editable), [fields])

  // Initialized ONCE per mount; the Inspector keys this component on
  // filePath+actionId, so a new selection remounts with a fresh draft, while a
  // post-Save version bump keeps the draft (it already equals the saved text).
  const [draft, setDraft] = useState<Record<string, string>>(() =>
    Object.fromEntries(editableFields.map((f) => [pathKey(f.path), f.value as string])),
  )
  // A persist that returns false (412 / yaml_error) leaves the buffer committed
  // in memory, so `dirty` flips back to false and the Save button locks. Flag it
  // (Fix #2) so the footer offers an "Open in Code view" escape instead of
  // stranding the user on a disabled Save.
  const [saveFailed, setSaveFailed] = useState(false)

  const handleFieldChange = useCallback((key: string, value: string) => {
    setSaveFailed(false)
    setDraft((d) => ({ ...d, [key]: value }))
  }, [])

  const changed = useMemo(
    () => editableFields.filter((f) => draft[pathKey(f.path)] !== (f.value as string)),
    [editableFields, draft],
  )
  const dirty = changed.length > 0

  const kind = action.kind ?? 'action'
  const kindStyle = KIND_STYLES[kind] ?? FALLBACK_KIND_STYLE

  const revert = useCallback(() => {
    setSaveFailed(false)
    setDraft(Object.fromEntries(editableFields.map((f) => [pathKey(f.path), f.value as string])))
  }, [editableFields])

  const save = useCallback(async () => {
    if (changed.length === 0) return
    setSaveFailed(false)
    // `commit` mutates the shared handle and live-syncs the new serialized YAML
    // into the buffer SYNCHRONOUSLY, so once it returns true the buffer already
    // holds the bytes to persist.
    const ok = commit((doc) => {
      for (const field of changed) setActionField(doc, actionId, field.path, draft[pathKey(field.path)])
    })
    if (!ok) {
      toast.error('Could not save — the document is read-only or has parse errors.')
      return
    }
    // commit only touches the in-memory buffer; write it through to disk (Fix
    // #9 — otherwise the edit was silently lost until re-saved from Code view).
    const persisted = await persistBufferToDisk(filePath, queryClient, runController)
    if (persisted) {
      onSaved?.()
    } else {
      // Persist failed (412 / yaml_error) but `commit` already cleared `dirty`,
      // so the Save button is now locked. Surface the in-modal recovery banner
      // (Fix #2) — the toast from persistBufferToDisk points to the Code view.
      setSaveFailed(true)
    }
  }, [changed, commit, actionId, draft, filePath, queryClient, runController, onSaved])

  return (
    <div className="px-3 py-3">
      <div className="flex items-center gap-2 pb-3">
        <span
          className={cn(
            'rounded px-1.5 py-0.5 text-2xs font-bold tracking-wider uppercase',
            kindStyle.chip,
          )}
        >
          {kind}
        </span>
        <span className="truncate font-mono text-xs font-semibold text-foreground" title={action.name}>
          {action.name === '' ? '(unnamed)' : action.name}
        </span>
      </div>

      {fields.length === 0 ? (
        <p className="text-xs text-muted-foreground">This action has no fields to show.</p>
      ) : (
        <div className="flex flex-col gap-2">
          {fields.map((field) => {
            const key = pathKey(field.path)
            return (
              <div key={key} className="grid grid-cols-[90px_minmax(0,1fr)] items-start gap-2">
                <label
                  htmlFor={field.editable ? `af-${key}` : undefined}
                  className="pt-1.5 font-mono text-2xs break-words text-muted-foreground"
                >
                  {field.label}
                </label>
                {field.editable ? (
                  field.multiline ? (
                    <Textarea
                      id={`af-${key}`}
                      value={draft[key] ?? ''}
                      disabled={readOnly}
                      onChange={(e) => handleFieldChange(key, e.target.value)}
                      className="min-h-[54px] resize-y font-mono text-xs"
                    />
                  ) : (
                    <Input
                      id={`af-${key}`}
                      value={draft[key] ?? ''}
                      disabled={readOnly}
                      onChange={(e) => handleFieldChange(key, e.target.value)}
                      className="h-7 font-mono text-xs"
                    />
                  )
                ) : (
                  <div className="min-h-7 rounded-sm border border-border bg-surface px-2 py-1 font-mono text-xs break-words text-foreground">
                    <DisplayValue value={field.value} />
                  </div>
                )}
              </div>
            )
          })}
        </div>
      )}

      {saveFailed && (
        <div
          role="alert"
          className="mt-3 flex items-start gap-2 rounded-sm border border-warning/30 bg-warning/10 px-2.5 py-2 text-2xs text-foreground"
        >
          <TriangleAlert className="mt-0.5 size-3.5 shrink-0 text-warning" aria-hidden="true" />
          <span className="flex-1">
            Couldn&apos;t save to disk — the file changed on disk or has a syntax error. Resolve it
            in the Code view.
          </span>
          <Button type="button" variant="outline" size="xs" onClick={() => onOpenCodeView?.()}>
            Open in Code view
          </Button>
        </div>
      )}

      <div className="mt-3 flex items-center gap-2 border-t border-border pt-2.5">
        <Button type="button" variant="outline" size="xs" disabled={!dirty} onClick={revert}>
          Revert
        </Button>
        <Button type="button" size="xs" disabled={!dirty || readOnly} onClick={() => void save()}>
          Save
        </Button>
        {dirty && (
          <span className="ml-auto flex items-center gap-1.5 text-2xs text-faint">
            <span className="size-1.5 rounded-full bg-warning" aria-hidden="true" />
            Editing action · unsaved
          </span>
        )}
      </div>
    </div>
  )
}
