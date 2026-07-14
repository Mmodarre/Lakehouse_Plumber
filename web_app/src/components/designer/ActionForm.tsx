import type { ReactNode } from 'react'
import { Layers, Plus, X } from 'lucide-react'
import { cn } from '@/lib/utils'
import { isPlainObject } from '@/lib/config-model'
import {
  appendActionListItem,
  deleteActionField,
  removeActionListItem,
  setActionField,
} from '@/lib/flowgroup-doc'
import type { ActionRead } from '@/lib/flowgroup-doc'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { BoolSwitch } from '@/components/config/fields/BoolSwitch'
import { DraftInput } from '@/components/config/fields/DraftInput'
import { EnumSelect } from '@/components/config/fields/EnumSelect'
import { KeyValueMapEditor } from '@/components/config/fields/KeyValueMapEditor'
import { OptionalNumberField } from '@/components/config/fields/OptionalNumberField'
import { OptionalTextField } from '@/components/config/fields/OptionalTextField'
import { StringListEditor } from '@/components/config/fields/StringListEditor'
import { FieldChrome } from '@/components/config/fields/FieldChrome'
import { SchemaKindProvider } from '@/components/common/SchemaKindContext'
import { hasTemplateParam, isPresent, isSubstitutionToken, readPath } from './specs/helpers'
import type { ActionSubTypeSpec, FieldSpec } from './specs/types'
import {
  coerceValue,
  computeIssues,
  enumOptions,
  fieldVisible,
  objectListRows,
  pathKey,
  visibleGroups,
  type DesignerMutator,
} from './formModel'
import { TokenField } from './TokenField'
import { codeFieldForPath } from './codeFields'
import { CodeFieldRow } from './ActionFormCodeField'
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

interface FieldRendererProps {
  field: FieldSpec
  raw: Record<string, unknown>
  actionId: string
  commit: (mutator: DesignerMutator) => void
  issue: string | undefined
  disabled: boolean
  /** A write is in flight — gates structural list/map controls (see ActionForm). */
  saving: boolean
  onEditCode: (target: CodeTarget) => void
}

/** Render one field via the right Config-UI widget, tolerating tokens. */
function FieldRenderer({
  field,
  raw,
  actionId,
  commit,
  issue,
  disabled,
  saving,
  onEditCode,
}: FieldRendererProps): ReactNode {
  const value = readPath(raw, field.path)
  // Namespace the DOM id by the owning action so stacked action cards in
  // FlowgroupFormView never collide (label htmlFor / aria-describedby would
  // otherwise all resolve to the first card's field). Unique per (action, path).
  const id = `af-${actionId}-${pathKey(field.path)}`
  const setKey = (val: unknown) =>
    commit((doc) => setActionField(doc, actionId, field.path, val))
  const delKey = () => commit((doc) => deleteActionField(doc, actionId, field.path))

  // Code fields (SQL/Python/expectations) add an editor / create-file affordance.
  const code = codeFieldForPath(field.path)
  if (code) {
    return (
      <CodeFieldRow
        id={id}
        field={field}
        code={code}
        value={value}
        actionId={actionId}
        issue={issue}
        disabled={disabled}
        onSet={setKey}
        onUnset={delKey}
        onEditCode={onEditCode}
      />
    )
  }

  // Widgets that can't hold a token fall back to TokenField so the value
  // round-trips (issue line becomes the token note). A `{{ param }}` renders
  // as the primary "param" chip; a `${env}`/`%{var}` token as the amber one.
  const token = (
    <TokenField
      id={id}
      label={field.label}
      value={String(value)}
      helpPath={field.path}
      onSet={setKey}
      onUnset={delKey}
      disabled={disabled}
      variant={hasTemplateParam(value) ? 'param' : 'substitution'}
    />
  )

  switch (field.widget) {
    case 'text':
    case 'textarea':
      // A value bound to a template parameter reads as a mono "param" chip.
      if (hasTemplateParam(value)) return token
      return (
        <OptionalTextField
          id={id}
          label={field.label}
          value={value}
          onSet={setKey}
          onUnset={delKey}
          helpPath={field.path}
          placeholder={field.placeholder}
          monospace={field.monospace}
          multiline={field.widget === 'textarea'}
          issue={issue}
          issueSeverity="warning"
          disabled={disabled}
        />
      )

    case 'number':
      if (isSubstitutionToken(value)) return token
      return (
        <OptionalNumberField
          id={id}
          label={field.label}
          value={value}
          min={field.min}
          max={field.max}
          onSet={setKey}
          onUnset={delKey}
          helpPath={field.path}
          placeholder={field.placeholder}
          issue={issue}
          disabled={disabled}
        />
      )

    case 'bool':
      if (isSubstitutionToken(value) || (value !== undefined && typeof value !== 'boolean')) {
        return token
      }
      return (
        <BoolSwitch
          id={id}
          label={field.label}
          value={value as boolean | undefined}
          defaultValue={field.defaultValue ?? false}
          onSet={setKey}
          onReset={delKey}
          helpPath={field.path}
          issue={issue}
          disabled={disabled}
        />
      )

    case 'enum': {
      const current = value === undefined ? field.enumDefault : String(value)
      return (
        <EnumSelect
          id={id}
          label={field.label}
          value={current}
          options={enumOptions(field, current)}
          unsetLabel={field.unsetLabel}
          onSet={(next) => {
            if (field.enumDefault !== undefined && next === field.enumDefault) delKey()
            else setKey(coerceValue(field, next))
          }}
          onUnset={field.unsetLabel ? delKey : undefined}
          helpPath={field.path}
          issue={issue}
          disabled={disabled}
        />
      )
    }

    case 'stringList':
      if (value !== undefined && !Array.isArray(value)) return token
      return renderStringList()

    case 'stringOrList':
      if (Array.isArray(value)) return renderStringList()
      if (value !== undefined && typeof value !== 'string') return token
      if (hasTemplateParam(value)) return token
      return (
        <OptionalTextField
          id={id}
          label={field.label}
          value={value}
          onSet={setKey}
          onUnset={delKey}
          helpPath={field.path}
          placeholder={field.placeholder}
          monospace={field.monospace}
          issue={issue}
          issueSeverity="warning"
          disabled={disabled}
        />
      )

    case 'keyValue':
      if (value !== undefined && !isPlainObject(value)) return token
      return (
        <KeyValueMapEditor
          id={id}
          label={field.label}
          value={value as Record<string, unknown> | undefined}
          onSetEntry={(key, val) =>
            commit((doc) => setActionField(doc, actionId, [...field.path, key], val))
          }
          onRenameEntry={(oldKey, newKey) =>
            commit((doc) => {
              const oldVal = readPath(raw, [...field.path, oldKey])
              deleteActionField(doc, actionId, [...field.path, oldKey])
              setActionField(doc, actionId, [...field.path, newKey], oldVal)
            })
          }
          onRemoveEntry={(key) =>
            commit((doc) => deleteActionField(doc, actionId, [...field.path, key]))
          }
          onDeleteKey={delKey}
          allowEmpty={field.allowEmpty}
          helpPath={field.path}
          issue={issue}
          issueSeverity="warning"
          disabled={disabled || saving}
        />
      )

    case 'objectList':
      // A malformed non-list value is left untouched and pointed at the YAML.
      if (value !== undefined && !Array.isArray(value)) return yamlNote()
      return renderObjectList()

    default:
      // Any not-yet-rendered widget: keep the value safe and point at the YAML.
      return yamlNote()
  }

  function yamlNote(): ReactNode {
    return (
      <FieldChrome id={id} label={field.label} helpPath={field.path} issue={issue} issueSeverity="warning">
        <p className="rounded-sm border border-dashed border-border px-2 py-1.5 text-2xs text-muted-foreground">
          {value === undefined ? 'Not set' : 'Value'} — edit this in the YAML for now.
        </p>
      </FieldChrome>
    )
  }

  function renderStringList(): ReactNode {
    const list = Array.isArray(value) ? value : undefined
    return (
      <StringListEditor
        id={id}
        label={field.label}
        value={list}
        onEditItem={(index, val) =>
          commit((doc) => setActionField(doc, actionId, [...field.path, index], val))
        }
        // Append resolves the insert index from the freshly-parsed doc, so a
        // double-add lands two items instead of colliding on a stale length.
        onAddItem={(val) => commit((doc) => appendActionListItem(doc, actionId, field.path, val))}
        // Delete-on-clear guard is read from the doc, not this render's list.
        onRemoveItem={(index) =>
          commit((doc) => removeActionListItem(doc, actionId, field.path, index, field.allowEmpty))
        }
        onDeleteKey={delKey}
        allowEmpty={field.allowEmpty}
        helpPath={field.path}
        placeholder={field.placeholder}
        monospace={field.monospace}
        issue={issue}
        disabled={disabled || saving}
      />
    )
  }

  // ── objectList — list of sub-objects, each row edited by `itemFields` ──
  // Add appends an empty row (creates the list when absent); deleting the
  // last row deletes the key (delete-on-clear). Each item field reuses the
  // per-widget renderer and commits at [...field.path, rowIndex, ...itemPath]
  // — the same immediate write-through as every other field.
  function renderObjectList(): ReactNode {
    const rows = objectListRows(raw, field.path)
    const itemFields = field.itemFields ?? []
    // Append resolves the row index from the freshly-parsed doc, so a
    // double-click on Add yields two rows instead of colliding on a stale
    // length. Add is NOT gated on `saving` — the two writes must both land.
    const addRow = () => commit((doc) => appendActionListItem(doc, actionId, field.path, {}))
    // Delete-on-clear guard is read from the doc; the remove button is gated
    // on `saving` so a racing second click cannot delete a shifted row.
    const removeRow = (index: number) =>
      commit((doc) => removeActionListItem(doc, actionId, field.path, index))
    return (
      <FieldChrome id={id} label={field.label} helpPath={field.path} issue={issue} issueSeverity="warning">
        {rows.length === 0 ? (
          <p className="text-2xs text-muted-foreground">No entries yet.</p>
        ) : (
          <ul className="space-y-2">
            {rows.map((row, index) => (
              // Index keys: rows are positional slots in the YAML sequence.
              <li key={index} className="space-y-2 rounded-sm border border-border p-2">
                <div className="flex items-center justify-between">
                  <span className="text-2xs font-medium text-muted-foreground">
                    {field.label} {index + 1}
                  </span>
                  <Button
                    type="button"
                    variant="ghost"
                    size="icon-sm"
                    onClick={() => removeRow(index)}
                    disabled={disabled || saving}
                    aria-label={`Remove ${field.label} entry ${index + 1}`}
                  >
                    <X aria-hidden="true" />
                  </Button>
                </div>
                {itemFields
                  .filter((itemField) => fieldVisible(itemField, row))
                  .map((itemField) => (
                    <FieldRenderer
                      key={pathKey(itemField.path)}
                      field={{ ...itemField, path: [...field.path, index, ...itemField.path] }}
                      raw={raw}
                      actionId={actionId}
                      commit={commit}
                      issue={
                        itemField.required && !isPresent(readPath(row, itemField.path))
                          ? `${itemField.label} is required.`
                          : undefined
                      }
                      disabled={disabled}
                      saving={saving}
                      onEditCode={onEditCode}
                    />
                  ))}
              </li>
            ))}
          </ul>
        )}
        <Button
          type="button"
          variant="outline"
          size="sm"
          className="gap-1"
          onClick={addRow}
          disabled={disabled}
          aria-label={`Add ${field.label} entry`}
        >
          <Plus aria-hidden="true" /> Add entry
        </Button>
      </FieldChrome>
    )
  }
}
