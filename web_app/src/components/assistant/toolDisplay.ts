import type { AssistantItem } from '../../types/assistant'

// Friendly one-line summaries for tool calls, shared by ToolCallCard and
// ApprovalCard so a pending approval and the finished call read the same.
// Unknown tools (incl. mcp__*) fall back to the raw tool name — never hide
// what is about to run.

export interface ToolDisplay {
  /** Short human verb ("Read", "Edit", "Command"). */
  label: string
  /** The load-bearing argument — path, command, pattern, url. */
  detail: string | null
}

/** Parse the `arguments` JSON string carried by `tool_call` items. */
export function parseToolArguments(item: AssistantItem): Record<string, unknown> {
  if (typeof item.arguments !== 'string') return {}
  try {
    const parsed: unknown = JSON.parse(item.arguments)
    return parsed !== null && typeof parsed === 'object'
      ? (parsed as Record<string, unknown>)
      : {}
  } catch {
    return {}
  }
}

function str(value: unknown): string | null {
  return typeof value === 'string' && value !== '' ? value : null
}

/** Render an absolute path relative to the served project root. */
export function shortenPath(
  path: string,
  root: string | null | undefined,
): string {
  if (!root) return path
  const prefix = root.endsWith('/') ? root : `${root}/`
  return path.startsWith(prefix) ? path.slice(prefix.length) : path
}

export function toolDisplay(
  name: string,
  args: Record<string, unknown>,
  root?: string | null,
): ToolDisplay {
  const path = (key: string = 'file_path') => {
    const value = str(args[key])
    return value === null ? null : shortenPath(value, root)
  }
  switch (name) {
    case 'Read':
    case 'NotebookRead':
      return { label: 'Read', detail: path() ?? path('notebook_path') }
    case 'Edit':
    case 'MultiEdit':
      return { label: 'Edit', detail: path() }
    case 'NotebookEdit':
      return { label: 'Edit', detail: path('notebook_path') }
    case 'Write':
      return { label: 'Write', detail: path() }
    case 'Bash':
      return { label: 'Command', detail: str(args.command) }
    case 'Glob':
      return { label: 'Find files', detail: str(args.pattern) }
    case 'Grep':
      return { label: 'Search', detail: str(args.pattern) }
    case 'WebFetch':
      return { label: 'Fetch', detail: str(args.url) }
    case 'WebSearch':
      return { label: 'Web search', detail: str(args.query) }
    case 'Task':
      return { label: 'Subagent', detail: str(args.description) }
    case 'TodoWrite': {
      const todos = Array.isArray(args.todos) ? args.todos.length : null
      return {
        label: 'Update todos',
        detail: todos === null ? null : `${todos} item${todos === 1 ? '' : 's'}`,
      }
    }
    default: {
      const first = Object.values(args).find(
        (value) => typeof value === 'string' && value !== '',
      )
      return { label: name, detail: typeof first === 'string' ? first : null }
    }
  }
}
