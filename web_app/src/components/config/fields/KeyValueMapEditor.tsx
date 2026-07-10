import { useState } from 'react'
import { Pencil, Plus, X } from 'lucide-react'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { DraftInput } from './DraftInput'
import { displayString, issueId } from './fieldSupport'

// ── KeyValueMapEditor — str→str map editor ───────────────────
//
// Editor for a string→string YAML map that never coerces what the user
// did not touch (plan-locked): rows whose existing value is NOT a string
// render read-only with a "not text" warning badge, and only become a
// string if the user unlocks ("Edit … as text") and edits THAT row.
// Value edits commit on blur/Enter as scalar patches; a key rename is a
// delete+set performed by the caller (the renamed entry moves to the end
// of the map — untouched rows keep their insertion order). Removing the
// last entry deletes the whole key unless `allowEmpty` is set.

export interface KeyValueMapEditorProps {
  /** DOM id prefix (label + issue wiring). */
  id: string
  label: string
  /** Current map; `undefined` = key absent. Insertion order is YAML order. */
  value: Record<string, unknown> | undefined
  /** Set one entry to a STRING value (also used for coercion-by-edit). */
  onSetEntry: (key: string, value: string) => void
  /** Rename an entry, PRESERVING its raw value (caller: delete old + set new). */
  onRenameEntry: (oldKey: string, newKey: string) => void
  /** Remove one entry. */
  onRemoveEntry: (key: string) => void
  /** Remove the whole key (last entry removed while `allowEmpty` is false). */
  onDeleteKey: () => void
  /** Keep `{}` in the file when the last entry is removed (default: delete the key). */
  allowEmpty?: boolean
  description?: string
  /** Map-level validation message. */
  issue?: string
  issueSeverity?: 'error' | 'warning'
  disabled?: boolean
}

export function KeyValueMapEditor({
  id,
  label,
  value,
  onSetEntry,
  onRenameEntry,
  onRemoveEntry,
  onDeleteKey,
  allowEmpty = false,
  description,
  issue,
  issueSeverity = 'error',
  disabled,
}: KeyValueMapEditorProps) {
  const [unlocked, setUnlocked] = useState<ReadonlySet<string>>(new Set())
  const [addKey, setAddKey] = useState('')
  const [addValue, setAddValue] = useState('')
  const [rowError, setRowError] = useState<string | null>(null)

  const entries = value !== undefined ? Object.entries(value) : undefined

  const removeRow = (key: string) => {
    if (entries !== undefined && entries.length === 1 && !allowEmpty) onDeleteKey()
    else onRemoveEntry(key)
  }
  const commitRename = (oldKey: string, next: string) => {
    const newKey = next.trim()
    if (newKey === '' || newKey === oldKey) return
    if (value !== undefined && newKey in value) {
      setRowError(`Key '${newKey}' already exists`)
      return
    }
    setRowError(null)
    onRenameEntry(oldKey, newKey)
  }
  const commitAdd = () => {
    const key = addKey.trim()
    if (key === '') return
    if (value !== undefined && key in value) {
      setRowError(`Key '${key}' already exists`)
      return
    }
    setRowError(null)
    onSetEntry(key, addValue)
    setAddKey('')
    setAddValue('')
  }

  return (
    <div className="space-y-1.5">
      <Label htmlFor={`${id}-add-key`} className="text-xs">
        {label}
      </Label>
      {description && <p className="text-2xs text-muted-foreground">{description}</p>}

      {entries === undefined ? (
        <p className="text-2xs text-muted-foreground">Not set</p>
      ) : entries.length === 0 ? (
        <p className="text-2xs text-muted-foreground">Empty map</p>
      ) : (
        <ul className="space-y-1">
          {entries.map(([key, raw]) => {
            const isString = typeof raw === 'string'
            const locked = !isString && !unlocked.has(key)
            return (
              <li key={key} className="flex items-center gap-1.5">
                <DraftInput
                  initial={key}
                  onCommit={(next) => commitRename(key, next)}
                  monospace
                  readOnly={locked}
                  disabled={disabled}
                  aria-label={`${key} key`}
                  className="max-w-48"
                />
                <DraftInput
                  initial={displayString(raw)}
                  onCommit={(next) => onSetEntry(key, next)}
                  monospace
                  readOnly={locked}
                  disabled={disabled}
                  aria-label={`${key} value`}
                />
                {!isString && (
                  <Badge
                    variant="outline"
                    className="shrink-0 rounded-sm border-warning/50 px-1.5 text-2xs text-warning"
                  >
                    not text
                  </Badge>
                )}
                {locked && (
                  <Button
                    type="button"
                    variant="ghost"
                    size="icon-sm"
                    onClick={() => setUnlocked((prev) => new Set(prev).add(key))}
                    disabled={disabled}
                    aria-label={`Edit ${key} as text`}
                    title="Unlock to edit — the value becomes a plain string"
                  >
                    <Pencil aria-hidden="true" />
                  </Button>
                )}
                <Button
                  type="button"
                  variant="ghost"
                  size="icon-sm"
                  onClick={() => removeRow(key)}
                  disabled={disabled}
                  aria-label={`Remove ${key}`}
                >
                  <X aria-hidden="true" />
                </Button>
              </li>
            )
          })}
        </ul>
      )}

      <div className="flex items-center gap-1.5">
        <Input
          id={`${id}-add-key`}
          value={addKey}
          onChange={(e) => setAddKey(e.target.value)}
          placeholder="key"
          spellCheck={false}
          autoComplete="off"
          disabled={disabled}
          className="max-w-48 font-mono text-xs"
          aria-label={`New ${label} key`}
        />
        <Input
          value={addValue}
          onChange={(e) => setAddValue(e.target.value)}
          onKeyDown={(e) => {
            if (e.key === 'Enter') {
              e.preventDefault()
              commitAdd()
            }
          }}
          placeholder="value"
          spellCheck={false}
          autoComplete="off"
          disabled={disabled}
          className="font-mono text-xs"
          aria-label={`New ${label} value`}
        />
        <Button
          type="button"
          variant="outline"
          size="icon-sm"
          onClick={commitAdd}
          disabled={disabled || addKey.trim() === ''}
          aria-label={`Add ${label} entry`}
        >
          <Plus aria-hidden="true" />
        </Button>
      </div>
      <p
        id={issueId(id)}
        role={rowError ?? issue ? 'alert' : undefined}
        className={
          rowError !== null || issueSeverity === 'error'
            ? 'text-2xs text-destructive'
            : 'text-2xs text-warning'
        }
      >
        {rowError ?? issue ?? ''}
      </p>
    </div>
  )
}
