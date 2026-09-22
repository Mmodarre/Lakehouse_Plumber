import type { RailSelection } from './docFormSupport'
import { AddButton } from './Rail'

interface SelectableDocument {
  index: number
  label: string
  caption?: string
  errors: number
  warnings: number
}

export function DocumentSelector({
  rail,
  selected,
  onSelect,
  canEdit,
  onAddSingle,
  onAddGroup,
  onAddDefaults,
  kind,
}: {
  rail: SelectableDocument[]
  selected: RailSelection
  onSelect: (selection: RailSelection) => void
  canEdit: boolean
  onAddSingle: () => void
  onAddGroup: () => void
  onAddDefaults?: () => void
  kind: 'pipeline' | 'job'
}) {
  return (
    <nav
      aria-label="Configuration documents"
      className="w-full space-y-2 rounded-md border border-border bg-card p-3"
    >
      <label className="flex flex-col gap-1 text-xs font-medium">
        Configuration document
        <select
          aria-label="Configuration document"
          value={String(selected)}
          onChange={(event) =>
            onSelect(event.target.value === 'builtin' ? 'builtin' : Number(event.target.value))
          }
          className="min-w-0 rounded-sm border border-border bg-background px-2 py-1.5 font-normal"
        >
          <option value="builtin">Built-in defaults · read-only</option>
          {rail.map((doc) => (
            <option key={doc.index} value={doc.index}>
              {doc.label} · {doc.caption}
              {doc.errors + doc.warnings ? ` · ${doc.errors + doc.warnings} issues` : ''}
            </option>
          ))}
        </select>
      </label>
      <p className="text-2xs text-muted-foreground">
        Built-in defaults → this file’s project_defaults → the selected {kind} override. Other{' '}
        {kind} documents are peers.
      </p>
      {canEdit && (
        <div className="flex flex-wrap gap-x-2">
          {onAddDefaults && <AddButton label="Add project defaults" onClick={onAddDefaults} />}
          <AddButton label={`Add ${kind}`} onClick={onAddSingle} />
          <AddButton
            label={kind === 'pipeline' ? 'Add group' : 'Add job group'}
            onClick={onAddGroup}
          />
        </div>
      )}
    </nav>
  )
}
