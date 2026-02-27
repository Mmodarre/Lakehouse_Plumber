/** Expandable card showing a tool call (file edit, grep, etc.) from the AI. */

import { useState } from 'react'
import type { ChatMessagePart, ToolState } from '../../types/chat'

// ── Tool name formatting ──────────────────────────────────────

const knownToolLabels: Record<string, string> = {
  read: 'Read File',
  write: 'Write File',
  edit: 'Edit File',
  grep: 'Search Code',
  glob: 'Find Files',
  bash: 'Run Command',
  task: 'Run Agent',
  webfetch: 'Fetch URL',
  websearch: 'Web Search',
  todoread: 'Read Todos',
  todowrite: 'Write Todos',
  notebookedit: 'Edit Notebook',
  question: 'Question',
  askuserquestion: 'Question',
}

/** Format MCP tool names like `mcp__serena__find_symbol` → "Serena: Find Symbol" */
function formatMcpName(name: string): string | null {
  const match = name.match(/^mcp__([^_]+)__(.+)$/)
  if (!match) return null
  const server = match[1].charAt(0).toUpperCase() + match[1].slice(1)
  const tool = match[2]
    .replace(/[-_]/g, ' ')
    .replace(/\b\w/g, (c) => c.toUpperCase())
  return `${server}: ${tool}`
}

function prettifyToolName(name: string): string {
  if (!name) return 'Tool'
  const lower = name.toLowerCase()
  if (knownToolLabels[lower]) return knownToolLabels[lower]
  const mcp = formatMcpName(name)
  if (mcp) return mcp
  return name
    .replace(/[-_]/g, ' ')
    .replace(/\b\w/g, (c) => c.toUpperCase())
}

/** When tool name is unknown, try to infer a useful label from args. */
function inferLabelFromArgs(args?: Record<string, unknown>): string | null {
  if (!args) return null
  if (typeof args.file_path === 'string') return 'File Operation'
  if (typeof args.command === 'string') return 'Run Command'
  if (typeof args.pattern === 'string') return 'Search'
  if (typeof args.url === 'string') return 'Web Request'
  if (typeof args.query === 'string') return 'Query'
  return null
}

// ── Contextual description ────────────────────────────────────

function getToolDescription(toolName: string, args?: Record<string, unknown>): string | null {
  if (!args) return null

  // OpenCode provides a title via state.title (stored as _title in args)
  if (typeof args._title === 'string' && args._title) {
    const title = args._title as string
    return title.length > 80 ? title.slice(0, 77) + '...' : title
  }

  const lower = toolName.toLowerCase()

  // File tools: show last 2 path segments
  if (['read', 'write', 'edit'].includes(lower)) {
    const fp = (args.file_path ?? args.path) as string | undefined
    if (fp) return fp.split('/').slice(-2).join('/')
  }

  // Bash: prefer description, fall back to truncated command
  if (lower === 'bash') {
    if (typeof args.description === 'string') return args.description
    if (typeof args.command === 'string') {
      const cmd = args.command as string
      return cmd.length > 60 ? cmd.slice(0, 57) + '...' : cmd
    }
  }

  // Search tools: show pattern
  if (lower === 'grep') {
    const pat = args.pattern as string | undefined
    const path = args.path as string | undefined
    if (pat) return path ? `"${pat}" in ${path.split('/').slice(-2).join('/')}` : `"${pat}"`
  }
  if (lower === 'glob') {
    const pat = args.pattern as string | undefined
    if (pat) return pat
  }

  // Web tools
  if (lower === 'webfetch' && typeof args.url === 'string') {
    return args.url.length > 60 ? args.url.slice(0, 57) + '...' : args.url
  }
  if (lower === 'websearch' && typeof args.query === 'string') {
    return `"${args.query}"`
  }

  // Generic fallback: file_path or path
  const fp = (args.file_path ?? args.path) as string | undefined
  if (typeof fp === 'string') return fp.split('/').slice(-2).join('/')

  return null
}

// ── Tool-specific icon ────────────────────────────────────────

type ToolCategory = 'file' | 'terminal' | 'search' | 'web' | 'agent' | 'generic'

function getToolCategory(toolName: string): ToolCategory {
  const lower = toolName.toLowerCase()
  if (['read', 'write', 'edit', 'notebookedit'].includes(lower)) return 'file'
  if (lower === 'bash') return 'terminal'
  if (['grep', 'glob'].includes(lower)) return 'search'
  if (['webfetch', 'websearch'].includes(lower)) return 'web'
  if (lower === 'task') return 'agent'
  // MCP tools — guess from name
  if (/read|write|edit|file/i.test(lower)) return 'file'
  if (/search|find|grep|glob|pattern/i.test(lower)) return 'search'
  return 'generic'
}

function statusDotColor(state: ToolState): string {
  if (state === 'pending' || state === 'running') return 'bg-blue-400 animate-pulse'
  if (state === 'error') return 'bg-red-400'
  return 'bg-green-400'
}

function ToolIcon({ toolName, state }: { toolName: string; state: ToolState }) {
  const category = getToolCategory(toolName)
  const isRunning = state === 'pending' || state === 'running'
  const color = isRunning ? 'text-blue-500' : state === 'error' ? 'text-red-400' : 'text-slate-400'

  const iconPath = {
    file: 'M7 21h10a2 2 0 002-2V9.414a1 1 0 00-.293-.707l-5.414-5.414A1 1 0 0012.586 3H7a2 2 0 00-2 2v14a2 2 0 002 2z',
    terminal: 'M8 9l3 3-3 3m5 0h3M5 20h14a2 2 0 002-2V6a2 2 0 00-2-2H5a2 2 0 00-2 2v12a2 2 0 002 2z',
    search: 'M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z',
    web: 'M21 12a9 9 0 01-9 9m9-9a9 9 0 00-9-9m9 9H3m9 9a9 9 0 01-9-9m9 9c1.657 0 3-4.03 3-9s-1.343-9-3-9m0 18c-1.657 0-3-4.03-3-9s1.343-9 3-9m-9 9a9 9 0 019-9',
    agent: 'M9.75 17L9 20l-1 1h8l-1-1-.75-3M3 13h18M5 17h14a2 2 0 002-2V5a2 2 0 00-2-2H5a2 2 0 00-2 2v10a2 2 0 002 2z',
    generic: 'M10.325 4.317c.426-1.756 2.924-1.756 3.35 0a1.724 1.724 0 002.573 1.066c1.543-.94 3.31.826 2.37 2.37a1.724 1.724 0 001.066 2.573c1.756.426 1.756 2.924 0 3.35a1.724 1.724 0 00-1.066 2.573c.94 1.543-.826 3.31-2.37 2.37a1.724 1.724 0 00-2.573 1.066c-.426 1.756-2.924 1.756-3.35 0a1.724 1.724 0 00-2.573-1.066c-1.543.94-3.31-.826-2.37-2.37a1.724 1.724 0 00-1.066-2.573c-1.756-.426-1.756-2.924 0-3.35a1.724 1.724 0 001.066-2.573c-.94-1.543.826-3.31 2.37-2.37.996.608 2.296.07 2.572-1.065z',
  }[category]

  return (
    <div className="relative shrink-0">
      <svg className={`h-3.5 w-3.5 ${color} ${isRunning ? 'animate-pulse' : ''}`} fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1.5}>
        <path strokeLinecap="round" strokeLinejoin="round" d={iconPath} />
      </svg>
      {/* Status dot — bottom-right corner */}
      <span className={`absolute -bottom-0.5 -right-0.5 block h-1.5 w-1.5 rounded-full ring-1 ring-slate-50 ${statusDotColor(state)}`} />
    </div>
  )
}

// ── Border color by state ─────────────────────────────────────

function borderColor(state: ToolState): string {
  if (state === 'pending' || state === 'running') return 'border-blue-200'
  if (state === 'error') return 'border-red-200'
  return 'border-slate-200'
}

// ── Component ─────────────────────────────────────────────────

export function ToolCallCard({ part }: { part: ChatMessagePart }) {
  const [expanded, setExpanded] = useState(false)

  const state: ToolState = part.toolState ?? 'completed'
  const rawName = part.toolName ?? ''
  const label = rawName ? prettifyToolName(rawName) : (inferLabelFromArgs(part.toolArgs) ?? 'Tool')
  const description = getToolDescription(rawName, part.toolArgs)
  // Filter out _title from display args (it's our internal metadata)
  const displayArgs = part.toolArgs
    ? Object.fromEntries(Object.entries(part.toolArgs).filter(([k]) => k !== '_title'))
    : undefined
  const hasArgs = !!displayArgs && Object.keys(displayArgs).length > 0
  const hasResult = !!part.toolResult
  const hasContent = hasArgs || hasResult

  return (
    <div className={`my-1 rounded border ${borderColor(state)} bg-slate-50 text-[11px]`}>
      {/* Header row */}
      <button
        onClick={() => setExpanded(!expanded)}
        className="flex w-full items-center gap-1.5 px-2 py-1.5 text-left hover:bg-slate-100"
        disabled={!hasContent}
      >
        <ToolIcon toolName={part.toolName ?? ''} state={state} />

        <span className="font-medium text-slate-600">{label}</span>

        {description && (
          <span className="truncate text-slate-400">{description}</span>
        )}

        {hasContent && (
          <svg
            className={`ml-auto h-3 w-3 shrink-0 text-slate-400 transition-transform ${expanded ? 'rotate-180' : ''}`}
            fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}
          >
            <path strokeLinecap="round" strokeLinejoin="round" d="M19 9l-7 7-7-7" />
          </svg>
        )}
      </button>

      {/* Error banner */}
      {state === 'error' && part.toolError && (
        <div className="border-t border-red-200 bg-red-50 px-2 py-1.5 text-[10px] text-red-600">
          {part.toolError}
        </div>
      )}

      {/* Expandable content: args + result */}
      {expanded && hasContent && (
        <div className="border-t border-slate-200 bg-white p-2">
          {hasArgs && (
            <pre className="max-h-48 overflow-auto whitespace-pre-wrap break-all font-mono text-[10px] text-slate-600">
              {JSON.stringify(displayArgs, null, 2)}
            </pre>
          )}
          {hasArgs && hasResult && (
            <hr className="my-1.5 border-slate-200" />
          )}
          {hasResult && (
            <pre className={`max-h-48 overflow-auto whitespace-pre-wrap break-all font-mono text-[10px] ${part.isError ? 'text-red-600' : 'text-slate-600'}`}>
              {part.toolResult}
            </pre>
          )}
        </div>
      )}
    </div>
  )
}
