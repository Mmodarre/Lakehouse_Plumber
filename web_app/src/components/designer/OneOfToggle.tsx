import { Fragment, useState, type ReactNode } from 'react'
import { SquarePen } from 'lucide-react'
import { deleteActionField, setActionField } from '@/lib/flowgroup-doc'
import { Button } from '@/components/ui/button'
import { OptionalTextField } from '@/components/config/fields/OptionalTextField'
import { SegmentedControl } from '@/components/ui/segmented-control'
import { isPresent, readPath } from './specs/helpers'
import type { FieldSpec } from './specs/types'
import { applyDiscriminatorChange, pathKey, type DesignerMutator } from './formModel'
import { FileRefField } from './FileRefField'
import type { CodeTarget } from './CodeModal'

// ── OneOfToggle — segmented "one-of" over branch-owned keys ──
//
// The control the ActionModalEditor shell renders for a `oneOfToggle` widget:
// a set of mutually-exclusive branches with NO backing discriminator key
// (inline `sql` ⊕ `sql_path`; a materialized view's `source` ⊕ `sql` ⊕
// `sql_path`; …). Each branch OWNS one YAML key and renders its own control
// per `backing` (inline code / file ref / plain text). It COMPOSES the
// existing pieces: `SegmentedControl` for the mode, plus the inline
// "Edit in editor" affordance (mirroring ActionFormCodeField's InlineCodeField)
// or `FileRefField` for the active branch.
//
// The mode is UI-LOCAL: an empty active branch has no YAML key to derive from,
// so state is seeded once from which branch key is present (else the first
// option) and driven by the user thereafter. Switching branches routes through
// `applyDiscriminatorChange` with NO discriminator write (`fieldPath`
// undefined) so it only PRUNES the now-inactive branches' keys — repairing the
// stale-branch XOR the Python validator rejects. Value edits commit at the
// active branch's path (delete-on-clear, like every other field).

export interface OneOfToggleProps {
  /** The `oneOfToggle` field spec (its `.oneOf.options` declare the branches). */
  spec: FieldSpec
  /** The action's raw mapping — read to seed the mode and the active value. */
  raw: Record<string, unknown>
  /** Canvas node id (mutator address). */
  actionId: string
  /** Staged write-through, shared with every other field. */
  commit: (mutator: DesignerMutator) => void
  /** Open the Monaco modal (inline body) or a file ref. */
  onEditCode: (target: CodeTarget) => void
  /** Editing is blocked (dirty text buffer / read-only). */
  disabled?: boolean
  /** Opt the active branch's string control into `${env}`-token autocomplete. */
  tokenComplete?: boolean
  /**
   * Soft-validation hint for the toggle as a whole (e.g. the `sql` ⊕ `sql_path`
   * xor rule). computeIssues re-keys a branch-path rule hint to this toggle's
   * synthetic path; rendered as a warning line under the control.
   */
  issue?: string
  /**
   * Render one nested sub-field (a `backing:'fields'` branch — e.g. snapshot
   * `source_function`'s file/function/parameters). Injected by FieldRenderer so
   * the shared renderer stays the single widget authority WITHOUT OneOfToggle
   * importing it back (which would cycle). Omit when no branch uses `'fields'`.
   */
  renderSubField?: (field: FieldSpec, issue: string | undefined) => ReactNode
}

export function OneOfToggle({
  spec,
  raw,
  actionId,
  commit,
  onEditCode,
  disabled = false,
  tokenComplete = false,
  issue,
  renderSubField,
}: OneOfToggleProps) {
  const options = spec.oneOf?.options ?? []

  // Seed the mode ONCE from which branch key is present (else the first
  // option). Lazy initializer so a later `raw` change (our own commit landing)
  // never overrides the user's chosen mode — the mode is UI-local.
  const [mode, setMode] = useState<string>(
    () => options.find((o) => isPresent(readPath(raw, o.path)))?.value ?? options[0]?.value ?? '',
  )

  const active = options.find((o) => o.value === mode) ?? options[0]
  if (!active) return null

  // Per-branch-value → owned-path map: the prune input. A switch keeps only the
  // new active branch's path and deletes every other branch's exclusive key.
  const branchPaths = Object.fromEntries(options.map((o) => [o.value, [o.path]]))

  const handleModeChange = (next: string) => {
    if (next === mode) return
    commit((doc) => applyDiscriminatorChange(doc, actionId, undefined, next, branchPaths))
    setMode(next)
  }

  const id = `af-${actionId}-${pathKey(active.path)}`
  const activeValue = readPath(raw, active.path)
  const setActive = (val: unknown) =>
    commit((doc) => setActionField(doc, actionId, active.path, val))
  const delActive = () => commit((doc) => deleteActionField(doc, actionId, active.path))

  return (
    <div className="space-y-2">
      <SegmentedControl
        value={mode}
        onValueChange={handleModeChange}
        options={options.map((o) => ({ value: o.value, label: o.label, disabled }))}
        aria-label={spec.label}
      />
      {active.backing === 'fields' ? (
        // Object-valued branch (e.g. source_function): render each declared
        // sub-field through the injected shared renderer. A required-missing
        // soft hint is surfaced per sub-field (mirroring the objectList row).
        <div className="space-y-3">
          {(active.fields ?? []).map((sub) => (
            <Fragment key={pathKey(sub.path)}>
              {renderSubField?.(
                sub,
                sub.required && !isPresent(readPath(raw, sub.path))
                  ? `${sub.label} is required.`
                  : undefined,
              )}
            </Fragment>
          ))}
        </div>
      ) : active.backing === 'file' ? (
        <FileRefField
          value={activeValue}
          onChange={(next) => (next === '' ? delActive() : setActive(next))}
          accept={active.accept ?? []}
          onEditCode={onEditCode}
        />
      ) : active.backing === 'inline' ? (
        <div className="space-y-1.5">
          <OptionalTextField
            id={id}
            label={active.label}
            value={activeValue}
            onSet={setActive}
            onUnset={delActive}
            helpPath={active.path}
            placeholder={active.placeholder}
            monospace
            multiline
            tokenComplete={tokenComplete}
            disabled={disabled}
          />
          <div className="flex justify-end">
            <Button
              type="button"
              variant="ghost"
              size="xs"
              disabled={disabled}
              onClick={() =>
                onEditCode({
                  backing: 'inline',
                  actionId,
                  path: active.path,
                  title: active.label,
                  language: active.language ?? 'sql',
                  initialValue: typeof activeValue === 'string' ? activeValue : '',
                })
              }
            >
              <SquarePen aria-hidden="true" />
              Edit in editor
            </Button>
          </div>
        </div>
      ) : (
        <OptionalTextField
          id={id}
          label={active.label}
          value={activeValue}
          onSet={setActive}
          onUnset={delActive}
          helpPath={active.path}
          placeholder={active.placeholder}
          tokenComplete={tokenComplete}
          disabled={disabled}
        />
      )}
      {issue && (
        <p role="alert" className="text-2xs text-warning">
          {issue}
        </p>
      )}
    </div>
  )
}
