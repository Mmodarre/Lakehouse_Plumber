/** Visual checklist card for todowrite / todoread tool calls.
 *
 * Parses todo items from tool args (write) or tool result (read)
 * and renders a styled task list instead of raw JSON.
 * Falls back to ToolCallCard if parsing fails.
 */

import type { ChatMessagePart, ToolState } from '../../types/chat'
import { ToolCallCard } from './ToolCallCard'

// ── Types ────────────────────────────────────────────────────

type TodoStatus = 'completed' | 'in_progress' | 'pending' | 'cancelled'
type TodoPriority = 'high' | 'medium' | 'low'

interface TodoItem {
  id?: string | number
  subject: string
  description?: string
  status: TodoStatus
  priority?: TodoPriority
}

// ── Helpers ──────────────────────────────────────────────────

/** Check whether a tool name represents a todo tool. */
export function isTodoTool(toolName?: string): boolean {
  if (!toolName) return false
  const lower = toolName.toLowerCase()
  return lower === 'todowrite' || lower === 'todoread'
}

function normalizeTodoStatus(raw: unknown): TodoStatus {
  if (typeof raw !== 'string') return 'pending'
  const lower = raw.toLowerCase()
  if (lower === 'completed' || lower === 'done') return 'completed'
  if (lower === 'in_progress' || lower === 'in-progress' || lower === 'running') return 'in_progress'
  if (lower === 'cancelled' || lower === 'canceled') return 'cancelled'
  return 'pending'
}

function normalizePriority(raw: unknown): TodoPriority | undefined {
  if (typeof raw !== 'string') return undefined
  const lower = raw.toLowerCase()
  if (lower === 'high') return 'high'
  if (lower === 'medium') return 'medium'
  if (lower === 'low') return 'low'
  return undefined
}

/** Try to extract a list of todo items from a raw value. */
function parseTodoItems(raw: unknown): TodoItem[] | null {
  if (!raw) return null

  // Could be a single object with `todos` key or a direct array
  let items: unknown[] | null = null

  if (Array.isArray(raw)) {
    items = raw
  } else if (typeof raw === 'object' && raw !== null) {
    const obj = raw as Record<string, unknown>
    // todowrite uses `todos` array in args
    if (Array.isArray(obj.todos)) items = obj.todos
    // Or it might be a `tasks` key
    else if (Array.isArray(obj.tasks)) items = obj.tasks
  }

  if (!items || items.length === 0) return null

  const result: TodoItem[] = []
  for (const item of items) {
    if (typeof item !== 'object' || item === null) return null
    const obj = item as Record<string, unknown>
    // Must have at least a subject/title/content
    const subject = (obj.subject ?? obj.title ?? obj.content ?? obj.task) as string | undefined
    if (typeof subject !== 'string') return null

    result.push({
      id: (obj.id ?? obj.index) as string | number | undefined,
      subject,
      description: typeof obj.description === 'string' ? obj.description : undefined,
      status: normalizeTodoStatus(obj.status ?? obj.state),
      priority: normalizePriority(obj.priority),
    })
  }

  return result
}

/** Try to parse todo items from toolResult (a JSON string from todoread). */
function parseTodoResult(toolResult?: string): TodoItem[] | null {
  if (!toolResult) return null
  try {
    const parsed = JSON.parse(toolResult)
    return parseTodoItems(parsed)
  } catch {
    return null
  }
}

// ── Status styling ──────────────────────────────────────────

function StatusIcon({ status }: { status: TodoStatus }) {
  switch (status) {
    case 'completed':
      return (
        <svg className="h-3.5 w-3.5 text-green-500 shrink-0" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
          <path strokeLinecap="round" strokeLinejoin="round" d="M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z" />
        </svg>
      )
    case 'in_progress':
      return (
        <span className="shrink-0 h-3.5 w-3.5 flex items-center justify-center">
          <span className="h-2 w-2 rounded-full bg-blue-400 animate-pulse" />
        </span>
      )
    case 'cancelled':
      return (
        <svg className="h-3.5 w-3.5 text-red-400 shrink-0" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
          <path strokeLinecap="round" strokeLinejoin="round" d="M10 14l2-2m0 0l2-2m-2 2l-2-2m2 2l2 2m7-2a9 9 0 11-18 0 9 9 0 0118 0z" />
        </svg>
      )
    default: // pending
      return (
        <span className="shrink-0 h-3.5 w-3.5 rounded-full border-2 border-slate-300" />
      )
  }
}

function PriorityBadge({ priority }: { priority: TodoPriority }) {
  const styles = {
    high: 'bg-red-50 text-red-600 ring-red-200',
    medium: 'bg-amber-50 text-amber-600 ring-amber-200',
    low: 'bg-slate-50 text-slate-500 ring-slate-200',
  }
  return (
    <span className={`text-[9px] font-medium px-1 py-0.5 rounded ring-1 ${styles[priority]}`}>
      {priority}
    </span>
  )
}

function statusBorder(state: ToolState): string {
  if (state === 'pending' || state === 'running') return 'border-blue-200'
  if (state === 'error') return 'border-red-200'
  return 'border-slate-200'
}

// ── Component ───────────────────────────────────────────────

export function TodoCard({ part }: { part: ChatMessagePart }) {
  const state: ToolState = part.toolState ?? 'completed'
  const isWrite = (part.toolName ?? '').toLowerCase() === 'todowrite'

  // Try args first (todowrite), then result (todoread)
  const items = parseTodoItems(part.toolArgs) ?? parseTodoResult(part.toolResult)

  // Fall back to generic ToolCallCard if parsing fails
  if (!items) {
    return <ToolCallCard part={part} />
  }

  const completedCount = items.filter((t) => t.status === 'completed').length
  const headerLabel = isWrite ? 'Task List Updated' : 'Current Tasks'

  return (
    <div className={`my-1 rounded border ${statusBorder(state)} bg-slate-50 text-[11px]`}>
      {/* Header */}
      <div className="flex items-center gap-1.5 px-2 py-1.5">
        <svg className="h-3.5 w-3.5 text-slate-400 shrink-0" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1.5}>
          <path strokeLinecap="round" strokeLinejoin="round" d="M9 5H7a2 2 0 00-2 2v12a2 2 0 002 2h10a2 2 0 002-2V7a2 2 0 00-2-2h-2M9 5a2 2 0 002 2h2a2 2 0 002-2M9 5a2 2 0 012-2h2a2 2 0 012 2m-6 9l2 2 4-4" />
        </svg>
        <span className="font-medium text-slate-600">{headerLabel}</span>
        <span className="ml-auto text-[10px] text-slate-400">
          {completedCount}/{items.length} done
        </span>
      </div>

      {/* Task list */}
      <div className="border-t border-slate-200 bg-white px-2 py-1.5 space-y-1">
        {items.map((item, idx) => (
          <div
            key={item.id ?? idx}
            className={`flex items-start gap-1.5 py-0.5 ${item.status === 'completed' ? 'opacity-60' : ''}`}
          >
            <StatusIcon status={item.status} />
            <div className="min-w-0 flex-1">
              <span className={`text-[11px] ${item.status === 'completed' ? 'line-through text-slate-400' : 'text-slate-700'}`}>
                {item.subject}
              </span>
              {item.description && (
                <p className="text-[10px] text-slate-400 leading-snug mt-0.5 line-clamp-2">
                  {item.description}
                </p>
              )}
            </div>
            {item.priority && item.priority !== 'low' && (
              <PriorityBadge priority={item.priority} />
            )}
          </div>
        ))}
      </div>
    </div>
  )
}
