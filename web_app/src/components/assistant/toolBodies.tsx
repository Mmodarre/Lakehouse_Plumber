import { Circle, CircleCheck, CircleDot } from 'lucide-react'
import { cn } from '../../lib/utils'

// Presentational building blocks for the per-tool renderer registry
// (`toolRenderers.tsx`). Kept in their own file so fast refresh sees a
// components-only module.

export const PRE_CLASS =
  'mt-1 max-h-48 overflow-auto rounded bg-muted p-2 text-2xs whitespace-pre-wrap text-foreground'

export function OutputPre({ output }: { output: string | null }) {
  if (output === null) return null
  return <pre className={PRE_CLASS}>{output}</pre>
}

export function CommandBlock({ command }: { command: string | null }) {
  if (command === null) return null
  return <pre className={cn(PRE_CLASS, 'font-mono')}>{command}</pre>
}

export function LineNumberedPreview({ text }: { text: string }) {
  return (
    <pre className={cn(PRE_CLASS, 'font-mono')}>
      {text.split('\n').map((line, i) => (
        <div key={i} className="flex gap-2">
          <span className="w-7 shrink-0 text-right text-muted-foreground select-none">
            {i + 1}
          </span>
          <span className="min-w-0 flex-1 break-all whitespace-pre-wrap">
            {line}
          </span>
        </div>
      ))}
    </pre>
  )
}

/** Full-block tinting for edits: removed text red, added text green. */
function TintedBlock({ text, tone }: { text: string; tone: 'removed' | 'added' }) {
  return (
    <pre
      className={cn(
        'max-h-48 overflow-auto rounded border p-2 font-mono text-2xs whitespace-pre-wrap text-foreground',
        tone === 'removed'
          ? 'border-error/30 bg-error/10'
          : 'border-success/30 bg-success/10',
      )}
    >
      {text}
    </pre>
  )
}

export function EditBlocks({
  oldString,
  newString,
}: {
  oldString: string | null
  newString: string | null
}) {
  if (oldString === null && newString === null) return null
  return (
    <div className="mt-1 flex flex-col gap-1">
      {oldString !== null && <TintedBlock text={oldString} tone="removed" />}
      {newString !== null && <TintedBlock text={newString} tone="added" />}
    </div>
  )
}

function TodoIcon({ status }: { status: string }) {
  if (status === 'completed') {
    return (
      <CircleCheck
        className="mt-px size-3 shrink-0 text-success"
        aria-label="completed"
      />
    )
  }
  if (status === 'in_progress') {
    return (
      <CircleDot
        className="mt-px size-3 shrink-0 text-primary"
        aria-label="in progress"
      />
    )
  }
  return (
    <Circle
      className="mt-px size-3 shrink-0 text-muted-foreground"
      aria-label="pending"
    />
  )
}

export function TodoChecklist({ todos }: { todos: unknown[] }) {
  return (
    <ul className="mt-1 flex flex-col gap-0.5">
      {todos.map((entry, i) => {
        const todo =
          entry !== null && typeof entry === 'object'
            ? (entry as Record<string, unknown>)
            : {}
        const status = typeof todo.status === 'string' ? todo.status : 'pending'
        return (
          <li key={i} className="flex items-start gap-1.5 text-2xs text-foreground">
            <TodoIcon status={status} />
            <span
              className={cn(
                'min-w-0 flex-1 break-words',
                status === 'completed' && 'text-muted-foreground line-through',
              )}
            >
              {typeof todo.content === 'string' ? todo.content : ''}
            </span>
          </li>
        )
      })}
    </ul>
  )
}
