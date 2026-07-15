import { useMemo } from 'react'
import { Loader2 } from 'lucide-react'
import { cn } from '@/lib/utils'
import { useOperationalMetadata } from '@/hooks/useOperationalMetadata'

// ── MetadataMultiSelect — operational-metadata column picker ──────────────────
//
// The control the ActionModalEditor shell renders for the `operational_metadata`
// field of load/transform actions (write actions get an informational note
// instead, per Decision 6 — NOT this widget). This is the ONE designer widget
// that touches the network: it calls `useOperationalMetadata()` DIRECTLY rather
// than receiving resolved options as a prop, so its host stays declarative.
//
// The value is tri-modal, matching `operational_metadata` in the action model
// (`Optional[Union[bool, List[str]]]`):
//   • undefined      → field absent (all columns off)
//   • true / false   → boolean "all-or-none" form
//   • string[]       → an explicit column allow-list
//
// `applies_to` is the write surface this action produces; only columns whose
// own `applies_to` array INCLUDES it are offered. A name present in an array
// `value` but NOT in the applicable catalogue (declared elsewhere, or removed
// from lhp.yaml) is preserved as a hinted chip — it must survive toggling other
// chips, never be silently dropped (round-trip fidelity).

export interface MetadataMultiSelectProps {
  /** Current `operational_metadata` value (tri-modal). */
  value: boolean | string[] | undefined
  /** Commit a new value: `true`/`false` for the boolean form, else a `string[]`. */
  onChange: (next: boolean | string[]) => void
  /** The write surface this action produces — filters the offered columns. */
  applies_to: 'view' | 'streaming_table' | 'materialized_view'
}

/** Chip pill styled after the artifact `.mchip`: a rounded card-surface pill;
 * the selected (`on`) state switches to the coral accent-weak fill + accent
 * line + a leading dot. */
function chipClass(selected: boolean): string {
  return cn(
    'inline-flex items-center gap-1.5 rounded-full border px-2.5 py-1 text-xs transition-colors',
    'focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring',
    selected
      ? 'border-accent-line bg-accent-weak font-medium text-primary'
      : 'border-border bg-card text-muted-foreground hover:text-foreground',
  )
}

export function MetadataMultiSelect({ value, onChange, applies_to }: MetadataMultiSelectProps) {
  const { data, isLoading, error } = useOperationalMetadata()

  const applicable = useMemo(
    () => (data?.columns ?? []).filter((c) => c.applies_to.includes(applies_to)),
    [data, applies_to],
  )

  // Names carried by an array value that are not applicable columns — kept so
  // they round-trip. Empty for the boolean / undefined forms.
  const unknownNames = useMemo(() => {
    if (!Array.isArray(value)) return []
    const applicableNames = new Set(applicable.map((c) => c.name))
    return value.filter((name) => !applicableNames.has(name))
  }, [value, applicable])

  const isAll = value === true
  const isNone = value === false

  const isOn = (name: string): boolean => {
    if (Array.isArray(value)) return value.includes(name)
    return value === true
  }

  // Every chip toggle emits a `string[]`. The base set materialises the current
  // mode into an explicit list (array→itself, incl. unknowns; `true`→all
  // applicable names; `false`/undefined→[]), then the toggled name is
  // added/removed. Toggling from `true` therefore converts to an explicit list
  // — the intended behaviour.
  const toggle = (name: string): void => {
    const base = Array.isArray(value)
      ? [...value]
      : value === true
        ? applicable.map((c) => c.name)
        : []
    const idx = base.indexOf(name)
    if (idx === -1) base.push(name)
    else base.splice(idx, 1)
    onChange(base)
  }

  if (isLoading) {
    return (
      <div className="flex items-center gap-1.5 text-xs text-muted-foreground">
        <Loader2 className="size-3 animate-spin" aria-hidden="true" />
        Loading operational-metadata columns…
      </div>
    )
  }

  if (error) {
    return (
      <p className="text-xs text-destructive">Could not load operational-metadata columns.</p>
    )
  }

  return (
    <div className="space-y-2">
      <div className="inline-flex items-center gap-1">
        <button
          type="button"
          aria-pressed={isAll}
          onClick={() => onChange(true)}
          className={cn(
            'rounded-md border px-2 py-0.5 text-xs transition-colors',
            isAll
              ? 'border-accent-line bg-accent-weak font-medium text-primary'
              : 'border-border bg-card text-muted-foreground hover:text-foreground',
          )}
        >
          All
        </button>
        <button
          type="button"
          aria-pressed={isNone}
          onClick={() => onChange(false)}
          className={cn(
            'rounded-md border px-2 py-0.5 text-xs transition-colors',
            isNone
              ? 'border-accent-line bg-accent-weak font-medium text-primary'
              : 'border-border bg-card text-muted-foreground hover:text-foreground',
          )}
        >
          None
        </button>
      </div>

      {applicable.length === 0 && unknownNames.length === 0 ? (
        <p className="text-xs text-muted-foreground">
          No operational-metadata columns available for this surface.
        </p>
      ) : (
        <div className="flex flex-wrap gap-1.5">
          {applicable.map((column) => (
            <button
              key={column.name}
              type="button"
              aria-pressed={isOn(column.name)}
              title={column.description ?? column.expression}
              onClick={() => toggle(column.name)}
              className={chipClass(isOn(column.name))}
            >
              {isOn(column.name) && (
                <span
                  className="size-1.5 rounded-full bg-primary"
                  aria-hidden="true"
                />
              )}
              {column.name}
            </button>
          ))}

          {unknownNames.map((name) => (
            <button
              key={name}
              type="button"
              aria-pressed
              title="Not defined in lhp.yaml — kept as-is"
              onClick={() => toggle(name)}
              className={chipClass(true)}
            >
              <span className="size-1.5 rounded-full bg-primary" aria-hidden="true" />
              {name}
              <span className="text-2xs font-normal text-muted-foreground">
                not defined in lhp.yaml
              </span>
            </button>
          ))}
        </div>
      )}
    </div>
  )
}
