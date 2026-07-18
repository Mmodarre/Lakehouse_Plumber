import type { ReactNode } from 'react'
import { Plus, X } from 'lucide-react'
import { isPlainObject } from '@/lib/config-model'
import {
  appendActionListItem,
  deleteActionField,
  removeActionListItem,
  setActionField,
} from '@/lib/flowgroup-doc'
import { Button } from '@/components/ui/button'
import { BoolSwitch } from '@/components/config/fields/BoolSwitch'
import { EnumSelect } from '@/components/config/fields/EnumSelect'
import { KeyValueMapEditor } from '@/components/config/fields/KeyValueMapEditor'
import { OptionalNumberField } from '@/components/config/fields/OptionalNumberField'
import { OptionalTextField } from '@/components/config/fields/OptionalTextField'
import { StringListEditor } from '@/components/config/fields/StringListEditor'
import { FieldChrome } from '@/components/config/fields/FieldChrome'
import { hasTemplateParam, isPresent, isSubstitutionToken, readPath } from './specs/helpers'
import type { FieldSpec } from './specs/types'
import {
  applyDiscriminatorChange,
  coerceValue,
  enumOptions,
  fieldVisible,
  objectListRows,
  pathKey,
  type DesignerMutator,
} from './formModel'
import { TokenField } from './TokenField'
import { codeFieldForPath, fileRefForField } from './codeFields'
import { CodeFieldRow } from './ActionFormCodeField'
import { DualSourceField } from './DualSourceField'
import { FileRefField } from './FileRefField'
import { OneOfToggle } from './OneOfToggle'
import type { CodeTarget } from './CodeModal'

// ── FieldRenderer — the shared per-field widget dispatch ─────
//
// Renders ONE spec field via the right Config-UI widget, tolerating tokens, and
// writes each committed edit through `commit`. Shared by both the (test-only)
// stacked `ActionForm` and the `ActionModalEditor` shell so every action field
// resolves through ONE renderer. Fields edit EXPLICIT YAML only: clearing a
// control DELETES the key (never writes '' / null); keys the spec does not name
// are never touched.

export interface FieldRendererProps {
  field: FieldSpec
  raw: Record<string, unknown>
  actionId: string
  commit: (mutator: DesignerMutator) => void
  issue: string | undefined
  disabled: boolean
  /** A write is in flight — gates structural list/map controls (see ActionForm). */
  saving: boolean
  onEditCode: (target: CodeTarget) => void
  /**
   * Opt every string input in this field into `${env}`-token autocomplete.
   * Off by default (ActionForm leaves it unset → byte-unchanged); the
   * ActionModalEditor shell turns it on so action string fields get `$`-completion.
   */
  tokenComplete?: boolean
}

export function FieldRenderer({
  field,
  raw,
  actionId,
  commit,
  issue,
  disabled,
  saving,
  onEditCode,
  tokenComplete = false,
}: FieldRendererProps): ReactNode {
  const value = readPath(raw, field.path)
  // A field is disabled when the caller says so OR its own `disabledWhen`
  // predicate holds for the current mapping (e.g. apply_as_truncates under SCD
  // Type 2). Additive — a field without `disabledWhen` keeps `disabled` exactly.
  const fieldDisabled = disabled || (field.disabledWhen?.(raw) ?? false)
  // Namespace the DOM id by the owning action so stacked action cards in
  // FlowgroupFormView never collide (label htmlFor / aria-describedby would
  // otherwise all resolve to the first card's field). Unique per (action, path).
  const id = `af-${actionId}-${pathKey(field.path)}`
  const setKey = (val: unknown) =>
    commit((doc) => setActionField(doc, actionId, field.path, val))
  const delKey = () => commit((doc) => deleteActionField(doc, actionId, field.path))

  // A one-of toggle owns its branches' keys itself (each branch commits at its
  // own path via applyDiscriminatorChange). Its `field.path` is synthetic, so
  // route it BEFORE the last-segment file/code heuristics below — those key
  // off the path's last segment and would otherwise misfire on the synthetic.
  if (field.widget === 'oneOfToggle' && field.oneOf) {
    return (
      <OneOfToggle
        spec={field}
        raw={raw}
        actionId={actionId}
        commit={commit}
        onEditCode={onEditCode}
        disabled={fieldDisabled}
        tokenComplete={tokenComplete}
        issue={issue}
        // A `'fields'`-backed branch renders its sub-fields through THIS shared
        // renderer (injected, so OneOfToggle needs no import back into
        // FieldRenderer). Each sub-field carries its own absolute path + issue.
        renderSubField={(subField, subIssue) => (
          <FieldRenderer
            field={subField}
            raw={raw}
            actionId={actionId}
            commit={commit}
            issue={subIssue}
            disabled={fieldDisabled}
            saving={saving}
            onEditCode={onEditCode}
            tokenComplete={tokenComplete}
          />
        )}
      />
    )
  }

  // File-ref fields (`sql_path`/`module_path`/`expectations_file`/explicit
  // `fileRef`) render a browse ⊕ create ⊕ edit control. Clearing the input
  // deletes the key (no `''` residue) — the delete-on-empty lives here at the
  // field level. `fileRefForField` returns null for an INLINE body (`sql`),
  // so inline code falls through to the code-field branch below.
  const ref = fileRefForField(field)
  if (ref) {
    return (
      <FileRefField
        value={value}
        onChange={(next) => (next === '' ? delKey() : setKey(next))}
        accept={ref.accept}
        baseDir={ref.baseDir}
        makeStub={ref.stub ? () => ref.stub!(raw) : undefined}
        suggestedPath={ref.suggestPath?.(raw) ?? undefined}
        placeholder={field.placeholder}
        onEditCode={onEditCode}
      />
    )
  }

  // Inline code bodies (SQL/Python/expectations) keep the Monaco "Edit in
  // editor" affordance; only file-backed refs route to FileRefField above.
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
        disabled={fieldDisabled}
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
      disabled={fieldDisabled}
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
          tokenComplete={tokenComplete}
          issue={issue}
          issueSeverity="warning"
          disabled={fieldDisabled}
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
          disabled={fieldDisabled}
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
          disabled={fieldDisabled}
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
          display={field.display}
          unsetLabel={field.unsetLabel}
          onSet={(next) => {
            // A discriminator carrying branchPaths prunes the now-inactive
            // branch's exclusive keys on switch (Task 0.5); it writes the raw
            // string value at field.path, which is correct for a string enum.
            // Absent branchPaths keeps the plain set/delete-to-default behavior.
            const branchPaths = field.branchPaths
            if (branchPaths) {
              commit((doc) => applyDiscriminatorChange(doc, actionId, field.path, next, branchPaths))
            } else if (field.enumDefault !== undefined && next === field.enumDefault) {
              delKey()
            } else {
              setKey(coerceValue(field, next))
            }
          }}
          onUnset={field.unsetLabel ? delKey : undefined}
          helpPath={field.path}
          issue={issue}
          disabled={fieldDisabled}
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
          tokenComplete={tokenComplete}
          issue={issue}
          issueSeverity="warning"
          disabled={fieldDisabled}
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
          disabled={fieldDisabled || saving}
        />
      )

    case 'objectList':
      // A malformed non-list value is left untouched and pointed at the YAML.
      if (value !== undefined && !Array.isArray(value)) return yamlNote()
      return renderObjectList()

    case 'dualSource':
      // Fixed-arity-2 compare (test row_count): editing either slot rewrites
      // the whole 2-item array; clearing BOTH slots prunes the key.
      return (
        <DualSourceField
          value={value}
          onChange={(next) => (next === undefined ? delKey() : setKey(next))}
        />
      )

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
        disabled={fieldDisabled || saving}
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
                    disabled={fieldDisabled || saving}
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
                      disabled={fieldDisabled}
                      saving={saving}
                      onEditCode={onEditCode}
                      tokenComplete={tokenComplete}
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
          disabled={fieldDisabled}
          aria-label={`Add ${field.label} entry`}
        >
          <Plus aria-hidden="true" /> Add entry
        </Button>
      </FieldChrome>
    )
  }
}
