import { Copy, Plus, Trash2 } from 'lucide-react'
import { Button } from '@/components/ui/button'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select'

// ── NodeActionsToolbar — structural ops for the selected node ─
//
// The compact toolbar above the inspector body: duplicate / delete the
// selected action, and (for actions that read a view source) fan-in via
// "Add input", which appends another producer's view to the source list.
// Add-downstream lives on the node's "+" instead; rename lives in the
// ActionForm header. All ops are disabled while read-only (the dirty-guard).

export interface NodeActionsToolbarProps {
  readOnly: boolean
  /** Producer views that can be added as an input (excludes self + current). */
  producerViews: string[]
  /** The action reads a string/list `source` (fan-in applies). */
  canAddInput: boolean
  onDuplicate: () => void
  onDelete: () => void
  onAddInput: (view: string) => void
}

export function NodeActionsToolbar({
  readOnly,
  producerViews,
  canAddInput,
  onDuplicate,
  onDelete,
  onAddInput,
}: NodeActionsToolbarProps) {
  const showAddInput = canAddInput && producerViews.length > 0
  return (
    <div className="flex items-center gap-1 border-b border-border px-4 py-2">
      {showAddInput && (
        // Controlled to "" so it always reads as a one-shot picker: choosing a
        // view fires onAddInput and the trigger falls back to its placeholder.
        <Select value="" onValueChange={(v) => !readOnly && onAddInput(v)}>
          <SelectTrigger
            size="sm"
            className="h-7 flex-1 text-2xs"
            disabled={readOnly}
            aria-label="Add input view"
          >
            <span className="flex items-center gap-1 text-muted-foreground">
              <Plus className="size-3" aria-hidden="true" />
              <SelectValue placeholder="Add input" />
            </span>
          </SelectTrigger>
          <SelectContent>
            {producerViews.map((view) => (
              <SelectItem key={view} value={view} className="font-mono text-xs">
                {view}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
      )}
      <div className="ml-auto flex items-center gap-1">
        <Button
          variant="ghost"
          size="icon-sm"
          className="text-muted-foreground hover:text-foreground"
          disabled={readOnly}
          onClick={onDuplicate}
          aria-label="Duplicate action"
          title="Duplicate action"
        >
          <Copy aria-hidden="true" />
        </Button>
        <Button
          variant="ghost"
          size="icon-sm"
          className="text-muted-foreground hover:text-destructive"
          disabled={readOnly}
          onClick={onDelete}
          aria-label="Delete action"
          title="Delete action"
        >
          <Trash2 aria-hidden="true" />
        </Button>
      </div>
    </div>
  )
}
