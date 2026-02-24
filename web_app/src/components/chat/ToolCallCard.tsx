/** Expandable card showing a tool call (file edit, grep, etc.) from the AI. */

import { useState } from 'react'
import type { ChatMessagePart } from '../../types/chat'

const toolLabels: Record<string, string> = {
  read: 'Read File',
  write: 'Write File',
  edit: 'Edit File',
  grep: 'Search Code',
  glob: 'Find Files',
  lhp_validate_yaml: 'Validate YAML',
  lhp_show_resolved: 'Show Resolved Config',
  lhp_list_presets: 'List Presets',
  lhp_list_templates: 'List Templates',
  lhp_get_substitutions: 'Get Substitutions',
}

export function ToolCallCard({ part }: { part: ChatMessagePart }) {
  const [expanded, setExpanded] = useState(false)

  const label = toolLabels[part.toolName ?? ''] ?? part.toolName ?? 'Tool'
  const isResult = part.type === 'tool-result'
  const hasContent = isResult ? !!part.toolResult : !!part.toolArgs

  return (
    <div className="my-1 rounded border border-slate-200 bg-slate-50 text-[11px]">
      <button
        onClick={() => setExpanded(!expanded)}
        className="flex w-full items-center gap-1.5 px-2 py-1.5 text-left hover:bg-slate-100"
        disabled={!hasContent}
      >
        {/* Icon */}
        <svg className="h-3 w-3 shrink-0 text-slate-400" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
          {isResult ? (
            part.isError
              ? <path strokeLinecap="round" strokeLinejoin="round" d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z" />
              : <path strokeLinecap="round" strokeLinejoin="round" d="M5 13l4 4L19 7" />
          ) : (
            <path strokeLinecap="round" strokeLinejoin="round" d="M10.325 4.317c.426-1.756 2.924-1.756 3.35 0a1.724 1.724 0 002.573 1.066c1.543-.94 3.31.826 2.37 2.37a1.724 1.724 0 001.066 2.573c1.756.426 1.756 2.924 0 3.35a1.724 1.724 0 00-1.066 2.573c.94 1.543-.826 3.31-2.37 2.37a1.724 1.724 0 00-2.573 1.066c-.426 1.756-2.924 1.756-3.35 0a1.724 1.724 0 00-2.573-1.066c-1.543.94-3.31-.826-2.37-2.37a1.724 1.724 0 00-1.066-2.573c-1.756-.426-1.756-2.924 0-3.35a1.724 1.724 0 001.066-2.573c-.94-1.543.826-3.31 2.37-2.37.996.608 2.296.07 2.572-1.065z" />
          )}
        </svg>

        <span className="font-medium text-slate-600">
          {isResult ? `${label} result` : label}
        </span>

        {/* File path if available */}
        {part.toolArgs?.path && (
          <span className="truncate text-slate-400">
            {String(part.toolArgs.path).split('/').slice(-2).join('/')}
          </span>
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

      {expanded && hasContent && (
        <div className="border-t border-slate-200 bg-white p-2">
          <pre className="max-h-48 overflow-auto whitespace-pre-wrap break-all font-mono text-[10px] text-slate-600">
            {isResult
              ? part.toolResult
              : JSON.stringify(part.toolArgs, null, 2)}
          </pre>
        </div>
      )}
    </div>
  )
}
