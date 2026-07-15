import { Layers } from 'lucide-react'
import { cn } from '@/lib/utils'
import { deleteActionField, setActionField } from '@/lib/flowgroup-doc'
import type { ActionRead } from '@/lib/flowgroup-doc'
import { Badge } from '@/components/ui/badge'
import { DraftInput } from '@/components/config/fields/DraftInput'
import { OptionalTextField } from '@/components/config/fields/OptionalTextField'
import { SchemaKindProvider } from '@/components/common/SchemaKindContext'
import type { ActionSubTypeSpec } from './specs/types'
import {
  computeIssues,
  fieldVisible,
  pathKey,
  visibleGroups,
  type DesignerMutator,
} from './formModel'
import { FieldRenderer } from './FieldRenderer'
import type { CodeTarget } from './CodeModal'

// ── ActionForm — the one form engine for every action spec ───
//
// Renders an ActionSubTypeSpec against a live action mapping and writes each
// committed edit through `commit`. Forms edit EXPLICIT YAML only: clearing a
// control DELETES the key (never writes '' / null), and keys the spec does
// not name are never touched. Soft-validation hints (required, cross-field)
// show on the field's issue line and never block a write.

/** Kind accent rail for the action card header. */
const KIND_RAIL: Record<string, string> = {
  load: 'bg-kind-load',
  transform: 'bg-kind-transform',
  write: 'bg-kind-write',
  test: 'bg-kind-test',
}

export interface ActionFormProps {
  spec: ActionSubTypeSpec
  /** The selected action, for reading current values. */
  action: ActionRead
  /** Mutator address (canvas node id — valid even for duplicate/unnamed names). */
  actionId: string
  /** Apply an edit + write through. */
  commit: (mutator: DesignerMutator) => void
  /** Rename the action; resolves true only when it persisted. */
  rename: (actionId: string, newName: string) => Promise<boolean>
  /** Disable every control (dirty text buffer or unresolved conflict). */
  readOnly: boolean
  /**
   * A write is in flight. Gates the structural list/map controls (add/remove
   * rows, add/rename map entries) so a second mutation cannot enqueue against a
   * stale index before the first write settles and the panel re-derives. Value
   * edits stay live.
   */
  saving?: boolean
  /** Preset names whose defaults affect this sub-type. */
  presetBadges: string[]
  /** Called after a rename so the canvas can re-select by the new node id. */
  onRenamed: (newName: string) => void
  /** Open a preset file as a text buffer (badge click). */
  onOpenPreset?: (name: string) => void
  /** Open the Monaco code modal for a SQL/Python/expectations field. */
  onEditCode: (target: CodeTarget) => void
}

export function ActionForm({
  spec,
  action,
  actionId,
  commit,
  rename,
  readOnly,
  saving = false,
  presetBadges,
  onRenamed,
  onOpenPreset,
  onEditCode,
}: ActionFormProps) {
  const raw = action.raw
  const issues = computeIssues(spec, raw)
  const groups = visibleGroups(spec, raw)

  const renameTo = (next: string) => {
    const trimmed = next.trim()
    if (trimmed === '' || trimmed === action.name) return
    // Re-select by the new name only once the rename actually persisted, so a
    // clash / conflict / read-only leaves the current selection intact.
    void rename(actionId, trimmed).then((ok) => {
      if (ok) onRenamed(trimmed)
    })
  }

  return (
    <div className="flex flex-col">
      <div className="border-b border-border px-4 pb-3 pt-4">
        <div className={cn('mb-2 h-0.5 w-8 rounded-full', KIND_RAIL[spec.kind] ?? 'bg-border')} />
        <DraftInput
          initial={action.name}
          onCommit={renameTo}
          monospace
          disabled={readOnly}
          aria-label="Action name"
          className="!text-sm font-semibold"
        />
        <div className="mt-1 text-2xs text-muted-foreground">
          {spec.kind} · {spec.title}
        </div>
        {spec.summary && <div className="mt-1 text-2xs text-muted-foreground">{spec.summary}</div>}
        {presetBadges.length > 0 && (
          <div className="mt-2 flex flex-wrap gap-1">
            {presetBadges.map((name) => (
              <Badge
                key={name}
                variant="outline"
                className={cn(
                  'gap-1 rounded-sm px-1.5 text-2xs font-normal text-muted-foreground',
                  onOpenPreset && 'cursor-pointer hover:text-foreground',
                )}
                onClick={onOpenPreset ? () => onOpenPreset(name) : undefined}
                title={`Affected by preset ${name}`}
              >
                <Layers className="size-2.5" aria-hidden="true" />
                {name}
              </Badge>
            ))}
          </div>
        )}
      </div>

      <SchemaKindProvider kind="flowgroup">
        <div className="flex flex-col gap-4 px-4 py-4">
          <OptionalTextField
            id={`af-${actionId}-description`}
            label="Description"
            value={raw.description}
            onSet={(value) => commit((doc) => setActionField(doc, actionId, ['description'], value))}
            onUnset={() => commit((doc) => deleteActionField(doc, actionId, ['description']))}
            disabled={readOnly}
          />

          {groups.map((group, gi) => {
            const fields = group.fields.filter((field) => fieldVisible(field, raw))
            if (fields.length === 0) return null
            return (
              <section key={group.title ?? gi} className="flex flex-col gap-3">
                {group.title && (
                  <div className="border-t border-border pt-3">
                    <div className="text-2xs font-semibold uppercase tracking-[0.05em] text-muted-foreground">
                      {group.title}
                    </div>
                    {group.description && (
                      <p className="mt-0.5 text-2xs text-muted-foreground">{group.description}</p>
                    )}
                  </div>
                )}
                {fields.map((field) => (
                  <FieldRenderer
                    key={pathKey(field.path)}
                    field={field}
                    raw={raw}
                    actionId={actionId}
                    commit={commit}
                    issue={issues.get(pathKey(field.path))}
                    disabled={readOnly}
                    saving={saving}
                    onEditCode={onEditCode}
                  />
                ))}
              </section>
            )
          })}
        </div>
      </SchemaKindProvider>
    </div>
  )
}
