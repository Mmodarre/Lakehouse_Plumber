import { useState } from 'react'
import { Input } from '@/components/ui/input'
import { Textarea } from '@/components/ui/textarea'
import { TokenAutocomplete } from '@/components/designer/TokenAutocomplete'
import { cn } from '../../../lib/utils'

// ── DraftInput — the commit-on-blur core of every text field ─
//
// Config forms must NEVER mutate the yaml-doc handle per keystroke: a
// keystroke-level setPath would mark the file dirty on the first key and
// spam the undo/diff surface. This input keeps a local draft and calls
// `onCommit` only on blur or Enter (single-line), and only when the text
// actually changed. Escape reverts the draft to the last committed value.

export interface DraftInputProps {
  /** The committed value the draft re-syncs from (external changes included). */
  initial: string
  /** Called on blur/Enter when the draft differs from `initial`. */
  onCommit: (next: string) => void
  id?: string
  /** Render a Textarea (blur-commit only; Enter inserts a newline). */
  multiline?: boolean
  monospace?: boolean
  /** Opt in to `${env}`-token autocomplete (a `$`-trigger popover overlay).
   * Default absent → the plain input; existing callsites are unaffected. */
  tokenComplete?: boolean
  placeholder?: string
  readOnly?: boolean
  disabled?: boolean
  'aria-label'?: string
  'aria-invalid'?: boolean
  'aria-describedby'?: string
  className?: string
}

export function DraftInput({
  initial,
  onCommit,
  multiline = false,
  monospace = false,
  tokenComplete = false,
  className,
  ...rest
}: DraftInputProps) {
  const [draft, setDraft] = useState(initial)
  // Re-sync when the committed value changes (our own commit landing, or an
  // external reload adopting new content while the field is not being
  // edited) — the render-phase state-adjustment pattern from the React docs.
  const [lastInitial, setLastInitial] = useState(initial)
  if (initial !== lastInitial) {
    setLastInitial(initial)
    setDraft(initial)
  }

  const commit = () => {
    if (draft !== initial) onCommit(draft)
  }
  const onKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter' && !multiline) {
      e.preventDefault()
      commit()
    } else if (e.key === 'Escape') {
      setDraft(initial)
    }
  }

  if (tokenComplete) {
    // TokenAutocomplete owns the keyboard while its popover is open (Enter =
    // select, Escape = close), and defers to the draft semantics below only
    // when it is closed — so `onKeyDown` is expressed as onCommit/onRevert.
    return (
      <TokenAutocomplete
        value={draft}
        onValueChange={setDraft}
        onCommit={commit}
        onRevert={() => setDraft(initial)}
        multiline={multiline}
        monospace={monospace}
        className={cn('text-xs', monospace && 'font-mono', className)}
        {...rest}
      />
    )
  }

  const shared = {
    value: draft,
    onChange: (e: React.ChangeEvent<HTMLInputElement | HTMLTextAreaElement>) =>
      setDraft(e.target.value),
    onBlur: commit,
    onKeyDown,
    spellCheck: false,
    autoComplete: 'off',
    className: cn('text-xs', monospace && 'font-mono', className),
    ...rest,
  }
  return multiline ? <Textarea rows={4} {...shared} /> : <Input {...shared} />
}
