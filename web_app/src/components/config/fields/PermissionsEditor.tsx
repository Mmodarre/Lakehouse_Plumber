import { Plus, X } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { Card, CardContent } from '@/components/ui/card'
import { Label } from '@/components/ui/label'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select'
import { DraftInput } from './DraftInput'
import { displayString } from './fieldSupport'

// ── PermissionsEditor — permissions[] entries ────────────────
//
// Generic list editor for Databricks-style permission entries: each entry
// needs a string `level` and EXACTLY ONE of user_name / group_name /
// service_principal_name (presence, not truthiness — mirrors the loader).
// The UI enforces the exactly-one rule structurally (radio per principal
// type; switching types deletes the old key and carries the value over)
// and still SURFACES validator issues for states loaded from YAML that
// the radios cannot represent (0 or 2+ principals: per-key prune rows).
// In the 2+ state the radios are DISABLED: switching would have to pick
// one value to carry and delete the rest, silently destroying data — the
// user prunes down to one entry first, then the radios come back.
//
// Deliberately path-agnostic (set/del take paths RELATIVE to the
// permissions key) so the pipeline editor binds it to
// `[...base, 'permissions']` and the job editor (Task 9) can rebind it
// unchanged.

export const PERMISSION_LEVELS = ['CAN_VIEW', 'CAN_RUN', 'CAN_MANAGE', 'CAN_MANAGE_RUN'] as const
const IDENTITY_KEYS = ['user_name', 'group_name', 'service_principal_name'] as const
const IDENTITY_LABELS: Record<(typeof IDENTITY_KEYS)[number], string> = {
  user_name: 'User',
  group_name: 'Group',
  service_principal_name: 'Service principal',
}

export interface PermissionsEditorProps {
  /** DOM id prefix. */
  id: string
  /** Raw value of the permissions key; `undefined` = key absent. */
  value: unknown
  /** Worst validation issue at a path RELATIVE to the permissions key. */
  issueAt: (
    rel: (string | number)[],
  ) => { message: string; severity: 'error' | 'warning' } | undefined
  /** Surgical set at a path relative to the permissions key ([] = the key itself). */
  set: (rel: (string | number)[], value: unknown) => void
  /** Surgical delete at a path relative to the permissions key. */
  del: (rel: (string | number)[]) => void
  /** Delete the whole permissions key (last entry removed — pristine absence). */
  onDeleteKey: () => void
  disabled?: boolean
}

function PermissionEntry({
  id,
  index,
  entry,
  issue,
  set,
  del,
  onRemove,
  disabled,
}: {
  id: string
  index: number
  entry: Record<string, unknown>
  issue?: { message: string; severity: 'error' | 'warning' }
  set: PermissionsEditorProps['set']
  del: PermissionsEditorProps['del']
  onRemove: () => void
  disabled?: boolean
}) {
  const present = IDENTITY_KEYS.filter((key) => key in entry)
  const level = typeof entry.level === 'string' ? entry.level : undefined
  const levelOptions =
    level !== undefined && !(PERMISSION_LEVELS as readonly string[]).includes(level)
      ? [level, ...PERMISSION_LEVELS]
      : [...PERMISSION_LEVELS]

  return (
    <Card className="gap-0 py-3">
      <CardContent className="space-y-3 px-4">
        <div className="flex items-center justify-between">
          <p className="text-2xs font-medium text-muted-foreground">Permission {index + 1}</p>
          <Button
            type="button"
            variant="ghost"
            size="icon-xs"
            aria-label={`Remove permission ${index + 1}`}
            onClick={onRemove}
            disabled={disabled}
          >
            <X aria-hidden="true" />
          </Button>
        </div>

        <div className="space-y-1.5">
          <Label htmlFor={`${id}-level`} className="text-xs">
            Level
          </Label>
          <Select
            value={level}
            onValueChange={(next) => set([index, 'level'], next)}
            disabled={disabled}
          >
            <SelectTrigger id={`${id}-level`} size="sm" className="w-full font-mono text-xs">
              <SelectValue placeholder="Select level…" />
            </SelectTrigger>
            <SelectContent>
              {levelOptions.map((option) => (
                <SelectItem key={option} value={option} className="font-mono text-xs">
                  {option}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>

        <fieldset className="space-y-1.5">
          <legend className="text-xs font-medium">Principal</legend>
          <div className="flex flex-wrap items-center gap-3">
            {IDENTITY_KEYS.map((key) => (
              <label key={key} className="flex items-center gap-1.5 text-xs">
                <input
                  type="radio"
                  name={`${id}-principal-type`}
                  checked={present.length === 1 && present[0] === key}
                  onChange={() => {
                    // Carry the value across; delete every other identity key.
                    // Only reachable from the exactly-one state (disabled
                    // below while 2+ keys exist), so nothing is destroyed.
                    const carried = present.length === 1 ? entry[present[0]!] : ''
                    for (const other of present) {
                      if (other !== key) del([index, other])
                    }
                    set([index, key], carried ?? '')
                  }}
                  disabled={disabled || present.length > 1}
                  aria-label={`${IDENTITY_LABELS[key]} principal for permission ${index + 1}`}
                />
                {IDENTITY_LABELS[key]}
              </label>
            ))}
          </div>
          {present.length > 1 && (
            <p className="text-2xs text-muted-foreground">
              Remove the extra principal entries below before switching the type.
            </p>
          )}
          {present.length === 1 && (
            <DraftInput
              initial={displayString(entry[present[0]!])}
              onCommit={(next) => set([index, present[0]!], next)}
              monospace
              placeholder={
                present[0] === 'group_name' ? 'data-engineers' : 'user@example.com'
              }
              aria-label={`${IDENTITY_LABELS[present[0]!]} name for permission ${index + 1}`}
              disabled={disabled}
            />
          )}
          {present.length > 1 &&
            present.map((key) => (
              <div key={key} className="flex items-center gap-2">
                <span className="w-40 shrink-0 font-mono text-2xs text-muted-foreground">
                  {key}
                </span>
                <span className="min-w-0 flex-1 truncate font-mono text-xs">
                  {displayString(entry[key])}
                </span>
                <Button
                  type="button"
                  variant="ghost"
                  size="icon-xs"
                  aria-label={`Remove ${key} from permission ${index + 1}`}
                  onClick={() => del([index, key])}
                  disabled={disabled}
                >
                  <X aria-hidden="true" />
                </Button>
              </div>
            ))}
        </fieldset>

        {issue && (
          <p role="alert" className="text-2xs text-destructive">
            {issue.message}
          </p>
        )}
      </CardContent>
    </Card>
  )
}

export function PermissionsEditor({
  id,
  value,
  issueAt,
  set,
  del,
  onDeleteKey,
  disabled,
}: PermissionsEditorProps) {
  const entries = Array.isArray(value) ? value : undefined
  const keyIssue = issueAt([])

  return (
    <div className="space-y-3">
      {keyIssue && (
        <p role="alert" className="text-2xs text-destructive">
          {keyIssue.message}
        </p>
      )}
      {entries?.map((entry, index) =>
        entry !== null && typeof entry === 'object' && !Array.isArray(entry) ? (
          <PermissionEntry
            key={index}
            id={`${id}-${index}`}
            index={index}
            entry={entry as Record<string, unknown>}
            issue={issueAt([index])}
            set={set}
            del={del}
            onRemove={() => (entries.length === 1 ? onDeleteKey() : del([index]))}
            disabled={disabled}
          />
        ) : (
          <p key={index} role="alert" className="text-2xs text-destructive">
            {issueAt([index])?.message ?? `Permissions entry ${index} must be a mapping`}
          </p>
        ),
      )}
      {entries === undefined && value === undefined && (
        <p className="text-2xs text-muted-foreground">Not set — no explicit permissions.</p>
      )}
      {/* Hidden while the key holds a non-list value: adding would clobber it. */}
      {(entries !== undefined || value === undefined) && (
        <Button
          type="button"
          variant="outline"
          size="sm"
          className="h-6 px-2 text-2xs"
          onClick={() =>
            entries === undefined
              ? set([], [{ level: 'CAN_MANAGE', user_name: '' }])
              : set([entries.length], { level: 'CAN_MANAGE', user_name: '' })
          }
          disabled={disabled}
        >
          <Plus aria-hidden="true" />
          Add permission
        </Button>
      )}
    </div>
  )
}
