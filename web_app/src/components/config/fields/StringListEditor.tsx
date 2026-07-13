import { useState } from 'react'
import { Plus, X } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import type { SchemaPath } from '@/lib/schema-help'
import { DraftInput } from './DraftInput'
import { FieldLabel } from './FieldLabel'
import { displayString, issueId } from './fieldSupport'

// ── StringListEditor — ordered list of strings ───────────────
//
// Ordered string-list editor with byte-surgical intent granularity: each
// row commits on blur/Enter through `onEditItem` (a scalar patch in
// yaml-doc — the other rows keep their exact bytes); add/remove go
// through their own callbacks (structural edits). The empty-vs-absent
// distinction is explicit: with `allowEmpty` unset (default), removing
// the last row calls `onDeleteKey` so the file goes back to not
// mentioning the key at all; with `allowEmpty`, `[]` stays in the file.
// Editing a row to an empty string removes that row.

export interface StringListEditorProps {
  /** DOM id prefix (label + issue wiring). */
  id: string
  label: string
  /** Current list; `undefined` = key absent. Non-string items display coerced. */
  value: readonly unknown[] | undefined
  /** Replace one item (yaml-doc patches just that scalar). */
  onEditItem: (index: number, value: string) => void
  /** Append one item (creates the list when absent). */
  onAddItem: (value: string) => void
  /** Remove one item. */
  onRemoveItem: (index: number) => void
  /** Remove the whole key (last row removed while `allowEmpty` is false). */
  onDeleteKey: () => void
  /** Keep `[]` in the file when the last row is removed (default: delete the key). */
  allowEmpty?: boolean
  /** Schema path the (i) tooltip resolves help from. */
  helpPath?: SchemaPath
  /** Explicit help override; wins over helpPath. */
  help?: string
  description?: string
  /** Placeholder for the add-item input. */
  placeholder?: string
  monospace?: boolean
  /** List-level validation message (e.g. "must be a list"). */
  issue?: string
  /** Per-item validation message, by index. */
  itemIssue?: (index: number) => string | undefined
  disabled?: boolean
}

export function StringListEditor({
  id,
  label,
  value,
  onEditItem,
  onAddItem,
  onRemoveItem,
  onDeleteKey,
  allowEmpty = false,
  helpPath,
  help,
  description,
  placeholder,
  monospace = false,
  issue,
  itemIssue,
  disabled,
}: StringListEditorProps) {
  const [addDraft, setAddDraft] = useState('')
  const items = Array.isArray(value) ? value : undefined

  const removeRow = (index: number) => {
    if (items !== undefined && items.length === 1 && !allowEmpty) onDeleteKey()
    else onRemoveItem(index)
  }
  const commitAdd = () => {
    const next = addDraft.trim()
    if (next === '') return
    onAddItem(next)
    setAddDraft('')
  }

  return (
    <div className="space-y-1.5">
      <FieldLabel
        htmlFor={`${id}-add`}
        label={label}
        helpPath={helpPath}
        help={help ?? description}
      />

      {items === undefined ? (
        <p className="text-2xs text-muted-foreground">Not set</p>
      ) : items.length === 0 ? (
        <p className="text-2xs text-muted-foreground">Empty list</p>
      ) : (
        <ul className="space-y-1">
          {items.map((item, index) => {
            const rowIssue = itemIssue?.(index)
            return (
              // Index keys are correct here: rows are positional slots in
              // the YAML sequence, and a remove must re-bind drafts by
              // position, not follow the removed value.
              <li key={index} className="space-y-0.5">
                <div className="flex items-center gap-1.5">
                  <DraftInput
                    initial={displayString(item)}
                    onCommit={(next) =>
                      next.trim() === '' ? removeRow(index) : onEditItem(index, next)
                    }
                    monospace={monospace}
                    disabled={disabled}
                    aria-label={`${label} item ${index + 1}`}
                    aria-invalid={rowIssue !== undefined ? true : undefined}
                  />
                  <Button
                    type="button"
                    variant="ghost"
                    size="icon-sm"
                    onClick={() => removeRow(index)}
                    disabled={disabled}
                    aria-label={`Remove ${label} item ${index + 1}`}
                  >
                    <X aria-hidden="true" />
                  </Button>
                </div>
                {rowIssue && (
                  <p role="alert" className="text-2xs text-destructive">
                    {rowIssue}
                  </p>
                )}
              </li>
            )
          })}
        </ul>
      )}

      <div className="flex items-center gap-1.5">
        <Input
          id={`${id}-add`}
          value={addDraft}
          onChange={(e) => setAddDraft(e.target.value)}
          onKeyDown={(e) => {
            if (e.key === 'Enter') {
              e.preventDefault()
              commitAdd()
            }
          }}
          placeholder={placeholder ?? 'Add item…'}
          spellCheck={false}
          autoComplete="off"
          disabled={disabled}
          className={monospace ? 'font-mono text-xs' : 'text-xs'}
          aria-describedby={issueId(id)}
        />
        <Button
          type="button"
          variant="outline"
          size="icon-sm"
          onClick={commitAdd}
          disabled={disabled || addDraft.trim() === ''}
          aria-label={`Add ${label} item`}
        >
          <Plus aria-hidden="true" />
        </Button>
      </div>
      <p id={issueId(id)} role={issue ? 'alert' : undefined} className="text-2xs text-destructive">
        {issue ?? ''}
      </p>
    </div>
  )
}
