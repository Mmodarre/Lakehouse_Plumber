import { OptionalTextField } from '@/components/config/fields/OptionalTextField'
import { dualSourceWrite, type DualSourceSlots } from './formModel'

// ── DualSourceField — row_count's fixed two-table compare ────
//
// The control the ActionModalEditor shell renders for a `dualSource` widget:
// the test `row_count` action compares exactly two tables (`source: [A, B]`).
// Unlike StringListEditor, arity is FIXED at two — no add/remove — so it is
// just two labeled, monospace, token-aware inputs (Task 1.4's `tokenComplete`
// seam) that read `value[0]`/`value[1]` and write the whole 2-item array.
//
// Composition over reimplementation: each slot is an OptionalTextField, and a
// slot edit routes through `dualSourceWrite` (the Task 0.4 model helper) which
// owns the compose/prune rule — it pins empty slots as `''` to keep the arity
// at two, and signals a key delete when BOTH slots end empty. A legacy non-2
// array reads back its first two elements without reshaping the file (the
// "exactly two" soft rule, added later, flags the mismatch).

export interface DualSourceFieldProps {
  /** Current field value — expected `string[]`; anything else degrades safely. */
  value: unknown
  /** Write the whole 2-item array, or `undefined` to prune the key. */
  onChange: (next: string[] | undefined) => void
}

const asStr = (x: unknown): string | undefined => (typeof x === 'string' ? x : undefined)

export function DualSourceField({ value, onChange }: DualSourceFieldProps) {
  const current: DualSourceSlots = Array.isArray(value)
    ? [asStr(value[0]), asStr(value[1])]
    : [undefined, undefined]

  const applyEdit = (index: 0 | 1, next: string) => {
    const mutation = dualSourceWrite(current, index, next)
    onChange(mutation.kind === 'delete' ? undefined : mutation.value)
  }

  return (
    <div className="space-y-3">
      <OptionalTextField
        id="dual-source-a"
        label="Table A"
        value={current[0]}
        monospace
        tokenComplete
        onSet={(v) => applyEdit(0, v)}
        onUnset={() => applyEdit(0, '')}
      />
      <OptionalTextField
        id="dual-source-b"
        label="Table B"
        value={current[1]}
        monospace
        tokenComplete
        onSet={(v) => applyEdit(1, v)}
        onUnset={() => applyEdit(1, '')}
      />
    </div>
  )
}
