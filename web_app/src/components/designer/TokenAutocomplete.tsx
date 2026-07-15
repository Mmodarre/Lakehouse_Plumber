import { useId, useLayoutEffect, useRef, useState } from 'react'
import { Braces } from 'lucide-react'
import { useEnvironmentResolved } from '@/hooks/useEnvironments'
import { useUIStore } from '@/store/uiStore'
import { cn } from '@/lib/utils'

// ── TokenAutocomplete — `$`-trigger completion for `${env}` tokens ─
//
// A string input that completes substitution tokens. Typing `$` (or clicking
// the trigger button at the input's right edge) opens a popover of the current
// environment's `${env}` tokens — name plus its resolved-value preview — from
// `useEnvironmentResolved(selectedEnv)`. Selecting one drops `${name}` at the
// caret. Secrets are NOT enumerated (there is no data source, per Decision 4):
// a single static row inserts the literal `${secret:scope/key}` template for
// the user to fill by hand.
//
// It is the token-aware body of `DraftInput` (opt-in via `tokenComplete`) and
// therefore owns the DRAFT lifecycle indirectly: it never commits on its own.
// `onValueChange` updates the draft; `onCommit`/`onRevert` are DraftInput's
// blur/Enter/Escape semantics, which this component defers to ONLY while the
// popover is closed — an open popover claims Enter (select) and Escape (close).
//
// Focus retention is the crux: DraftInput commits on blur, so a popover click
// must not blur the input. Options are non-focusable rows whose `onMouseDown`
// is prevented, keeping focus (and the caret) in the input while the value is
// spliced. The list is exposed via `aria-activedescendant`, so keyboard users
// navigate with the arrows without focus ever leaving the field.

export interface TokenAutocompleteProps {
  /** Current text (the DraftInput draft in the real seam). */
  value: string
  /** Update the value/draft. NEVER commits. */
  onValueChange: (next: string) => void
  /** Blur / Enter (single-line) commit — invoked only when the popover is closed. */
  onCommit: () => void
  /** Escape revert — invoked only when the popover is closed. */
  onRevert: () => void
  multiline?: boolean
  monospace?: boolean
  className?: string
  id?: string
  placeholder?: string
  disabled?: boolean
  readOnly?: boolean
  'aria-label'?: string
  'aria-invalid'?: boolean
  'aria-describedby'?: string
}

type TokenOption =
  | { kind: 'token'; key: string; label: string; preview: string; insert: string }
  | { kind: 'secret'; key: string; label: string; preview: string; insert: string }

const SECRET_INSERT = '${secret:scope/key}'

const INPUT_CLASSES =
  'h-9 w-full min-w-0 rounded-md border border-input bg-transparent py-1 pr-9 pl-3 text-base shadow-xs outline-none placeholder:text-muted-foreground disabled:pointer-events-none disabled:cursor-not-allowed disabled:opacity-50 md:text-sm dark:bg-input/30 focus-visible:border-ring focus-visible:ring-[3px] focus-visible:ring-ring/50 aria-invalid:border-destructive aria-invalid:ring-destructive/20 dark:aria-invalid:ring-destructive/40'

const TEXTAREA_CLASSES =
  'flex field-sizing-content min-h-16 w-full rounded-md border border-input bg-transparent py-2 pr-9 pl-3 text-base shadow-xs outline-none placeholder:text-muted-foreground disabled:cursor-not-allowed disabled:opacity-50 md:text-sm dark:bg-input/30 focus-visible:border-ring focus-visible:ring-[3px] focus-visible:ring-ring/50 aria-invalid:border-destructive aria-invalid:ring-destructive/20 dark:aria-invalid:ring-destructive/40'

/** The incomplete `${…}` fragment ending at `caret`, if any. `start` is the
 * index of the opening `$` (what a selection replaces); `query` is the token
 * text typed so far. `null` when the caret is not inside a token trigger —
 * completed `}`, whitespace, or a `$` not followed by `{[word]`. */
function findTrigger(text: string, caret: number): { start: number; query: string } | null {
  for (let i = caret - 1; i >= 0; i--) {
    const ch = text[i]
    if (ch === '$') {
      const between = text.slice(i + 1, caret)
      if (between === '') return { start: i, query: '' }
      const m = /^\{([A-Za-z0-9_]*)$/.exec(between)
      return m ? { start: i, query: m[1] } : null
    }
    if (ch === '}' || ch === ' ' || ch === '\n' || ch === '\r' || ch === '\t') return null
  }
  return null
}

export function TokenAutocomplete({
  value,
  onValueChange,
  onCommit,
  onRevert,
  multiline = false,
  monospace = false,
  className,
  id,
  placeholder,
  disabled,
  readOnly,
  'aria-label': ariaLabel,
  'aria-invalid': ariaInvalid,
  'aria-describedby': ariaDescribedby,
}: TokenAutocompleteProps) {
  const env = useUIStore((s) => s.selectedEnv)
  const { data } = useEnvironmentResolved(env)
  const tokens = data?.tokens ?? {}

  const [open, setOpen] = useState(false)
  const [query, setQuery] = useState('')
  const [activeIndex, setActiveIndex] = useState(0)

  const inputRef = useRef<HTMLInputElement | HTMLTextAreaElement>(null)
  // Set right before a value update to restore the caret after the controlled
  // re-render lands (the parent owns `value`, so React would otherwise leave
  // the caret at the end).
  const pendingCaret = useRef<number | null>(null)

  const baseId = useId()
  const listId = `${baseId}-list`
  const optionId = (i: number) => `${baseId}-opt-${i}`

  const q = query.toLowerCase()
  const options: TokenOption[] = [
    ...Object.entries(tokens)
      .filter(([name]) => name.toLowerCase().includes(q))
      .sort(([a], [b]) => a.localeCompare(b))
      .map(
        ([name, resolved]): TokenOption => ({
          kind: 'token',
          key: name,
          label: '${' + name + '}',
          preview: resolved,
          insert: '${' + name + '}',
        }),
      ),
    ...(q === '' || 'secret'.includes(q)
      ? [
          {
            kind: 'secret' as const,
            key: '__secret__',
            label: SECRET_INSERT,
            preview: 'secret reference',
            insert: SECRET_INSERT,
          },
        ]
      : []),
  ]
  const showList = open && options.length > 0
  const active = Math.min(activeIndex, Math.max(0, options.length - 1))

  useLayoutEffect(() => {
    const el = inputRef.current
    if (pendingCaret.current !== null && el) {
      el.setSelectionRange(pendingCaret.current, pendingCaret.current)
      pendingCaret.current = null
    }
  })

  const selectOption = (opt: TokenOption) => {
    const el = inputRef.current
    const caret = el?.selectionStart ?? value.length
    const trigger = findTrigger(value, caret)
    const from = trigger ? trigger.start : caret
    const next = value.slice(0, from) + opt.insert + value.slice(caret)
    pendingCaret.current = from + opt.insert.length
    setOpen(false)
    onValueChange(next)
    el?.focus()
  }

  const handleChange = (e: React.ChangeEvent<HTMLInputElement | HTMLTextAreaElement>) => {
    const next = e.target.value
    const caret = e.target.selectionStart ?? next.length
    onValueChange(next)
    const trigger = findTrigger(next, caret)
    if (trigger) {
      setOpen(true)
      setQuery(trigger.query)
      setActiveIndex(0)
    } else {
      setOpen(false)
    }
  }

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (open) {
      if (e.key === 'Escape') {
        // Belongs to the popover — never DraftInput's revert.
        e.preventDefault()
        e.stopPropagation()
        setOpen(false)
        return
      }
      if (showList) {
        if (e.key === 'ArrowDown') {
          e.preventDefault()
          setActiveIndex((i) => (Math.min(i, options.length - 1) + 1) % options.length)
          return
        }
        if (e.key === 'ArrowUp') {
          e.preventDefault()
          setActiveIndex(
            (i) => (Math.min(i, options.length - 1) - 1 + options.length) % options.length,
          )
          return
        }
        if (e.key === 'Enter') {
          e.preventDefault()
          selectOption(options[active])
          return
        }
      }
    }
    // Popover closed (or open-but-empty): DraftInput's own semantics.
    if (e.key === 'Enter' && !multiline) {
      e.preventDefault()
      onCommit()
    } else if (e.key === 'Escape') {
      onRevert()
    }
  }

  const handleBlur = () => {
    setOpen(false)
    onCommit()
  }

  const openFromButton = () => {
    setQuery('')
    setActiveIndex(0)
    setOpen(true)
    inputRef.current?.focus()
  }

  const inputProps = {
    id,
    value,
    placeholder,
    disabled,
    readOnly,
    spellCheck: false,
    autoComplete: 'off',
    role: 'combobox',
    'aria-label': ariaLabel,
    'aria-invalid': ariaInvalid,
    'aria-describedby': ariaDescribedby,
    'aria-expanded': open,
    'aria-controls': listId,
    'aria-autocomplete': 'list' as const,
    'aria-activedescendant': showList ? optionId(active) : undefined,
    onChange: handleChange,
    onKeyDown: handleKeyDown,
    onBlur: handleBlur,
  }

  return (
    <div className="relative">
      {multiline ? (
        <textarea
          ref={inputRef as React.RefObject<HTMLTextAreaElement>}
          rows={4}
          aria-multiline="true"
          className={cn(TEXTAREA_CLASSES, monospace && 'font-mono', className)}
          {...inputProps}
        />
      ) : (
        <input
          ref={inputRef as React.RefObject<HTMLInputElement>}
          type="text"
          className={cn(INPUT_CLASSES, monospace && 'font-mono', className)}
          {...inputProps}
        />
      )}

      <button
        type="button"
        aria-label="Insert token"
        title="Insert ${…} token"
        disabled={disabled}
        // Keep focus (and the caret) in the input while opening.
        onMouseDown={(e) => e.preventDefault()}
        onClick={openFromButton}
        className={cn(
          'absolute right-1 flex size-6 items-center justify-center rounded-sm text-muted-foreground hover:text-foreground focus-visible:ring-[2px] focus-visible:ring-ring/50 focus-visible:outline-none disabled:opacity-50',
          multiline ? 'top-1.5' : 'top-1/2 -translate-y-1/2',
        )}
      >
        <Braces className="size-3.5" aria-hidden="true" />
      </button>

      {showList && (
        <ul
          id={listId}
          role="listbox"
          aria-label="Substitution tokens"
          className="absolute z-50 mt-1 max-h-60 w-full overflow-auto rounded-md border bg-popover p-1 text-popover-foreground shadow-md motion-safe:animate-in motion-safe:fade-in-0 motion-safe:duration-150"
        >
          {options.map((opt, i) => (
            <li
              key={opt.key}
              id={optionId(i)}
              role="option"
              aria-selected={i === active}
              // preventDefault keeps focus in the input (no blur-commit) while
              // the click still fires to run the insertion.
              onMouseDown={(e) => e.preventDefault()}
              onClick={() => selectOption(opt)}
              onMouseEnter={() => setActiveIndex(i)}
              className={cn(
                'flex cursor-pointer items-center gap-2 rounded-sm px-2 py-1.5 text-xs',
                i === active && 'bg-accent text-accent-foreground',
              )}
            >
              <span className="font-mono">{opt.label}</span>
              <span
                className={cn(
                  'ml-auto truncate pl-2 text-2xs',
                  opt.kind === 'secret'
                    ? 'text-muted-foreground italic'
                    : 'font-mono text-muted-foreground',
                )}
              >
                {opt.preview}
              </span>
            </li>
          ))}
        </ul>
      )}
    </div>
  )
}
