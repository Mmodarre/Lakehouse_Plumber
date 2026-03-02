import type { ActionCatalogEntry } from '../types/builder'
import { ACTION_TYPE_COLORS } from '../hooks/useActionCatalog'

interface PaletteItemProps {
  entry: ActionCatalogEntry
}

export function PaletteItem({ entry }: PaletteItemProps) {
  const colors = ACTION_TYPE_COLORS[entry.type]

  const onDragStart = (event: React.DragEvent) => {
    event.dataTransfer.setData('application/lhp-action-type', entry.type)
    event.dataTransfer.setData('application/lhp-action-subtype', entry.subtype)
    event.dataTransfer.effectAllowed = 'move'
  }

  return (
    <div
      draggable
      onDragStart={onDragStart}
      className={`cursor-grab rounded border px-3 py-2 transition-colors hover:shadow-sm active:cursor-grabbing ${colors.bg} ${colors.border}`}
    >
      <div className={`text-xs font-medium ${colors.text}`}>{entry.label}</div>
      <div className="text-[10px] text-slate-500">{entry.description}</div>
    </div>
  )
}
