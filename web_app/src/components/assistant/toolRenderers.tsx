import type { ReactNode } from 'react'
import {
  CommandBlock,
  EditBlocks,
  LineNumberedPreview,
  OutputPre,
  PRE_CLASS,
  TodoChecklist,
} from './toolBodies'
import { shortenPath } from './toolDisplay'

// Per-tool renderer registry, shared by ToolCallCard, ToolCallGroup and
// ApprovalCard so a pending approval and the finished call read the same.
// Unknown tools (incl. mcp__*) fall back to the raw tool name plus a
// raw-JSON disclosure — never hide what is about to run.

export interface ToolHeader {
  /** Card headline ("Read", or Bash's own description). */
  title: string
  /** The load-bearing argument — command, path, pattern, url (monospace). */
  subtitle?: string | null
}

export interface ToolBodyProps {
  args: Record<string, unknown>
  /** Clipped tool output (`output_preview`); null until the call is done. */
  output: string | null
  root?: string | null
}

export interface ToolRenderer {
  /** Short verb for group summaries ("Read", "Command", "Search"). */
  label: string
  display: (args: Record<string, unknown>, root?: string | null) => ToolHeader
  /** Returns null when there is nothing to disclose (cards call these as
   * plain functions and skip the disclosure on null). */
  Body?: (props: ToolBodyProps) => ReactNode
  ApprovalBody?: (props: {
    args: Record<string, unknown>
    root?: string | null
  }) => ReactNode
  /** True on the fallback entry — the cards show the raw-JSON disclosure. */
  unknown?: boolean
}

function str(value: unknown): string | null {
  return typeof value === 'string' && value !== '' ? value : null
}

function pathArg(
  args: Record<string, unknown>,
  root: string | null | undefined,
  key = 'file_path',
): string | null {
  const value = str(args[key])
  return value === null ? null : shortenPath(value, root)
}

function editBody(args: Record<string, unknown>): ReactNode {
  const oldString = str(args.old_string)
  const newString = str(args.new_string)
  if (oldString === null && newString === null) return null
  return <EditBlocks oldString={oldString} newString={newString} />
}

function multiEditBody(args: Record<string, unknown>): ReactNode {
  const edits = Array.isArray(args.edits) ? args.edits : []
  if (edits.length === 0) return null
  return (
    <div className="mt-1 flex flex-col gap-2">
      {edits.map((edit, i) => {
        const e =
          edit !== null && typeof edit === 'object'
            ? (edit as Record<string, unknown>)
            : {}
        return (
          <EditBlocks
            key={i}
            oldString={str(e.old_string)}
            newString={str(e.new_string)}
          />
        )
      })}
    </div>
  )
}

/** All-new content (Write / NotebookEdit): green only. */
function allNewBody(content: string | null): ReactNode {
  if (content === null) return null
  return <EditBlocks oldString={null} newString={content} />
}

const readRenderer: ToolRenderer = {
  label: 'Read',
  display: (args, root) => ({
    title: 'Read',
    subtitle: pathArg(args, root) ?? pathArg(args, root, 'notebook_path'),
  }),
  Body: ({ output }) =>
    output === null ? null : <LineNumberedPreview text={output} />,
}

const editRenderer: ToolRenderer = {
  label: 'Edit',
  display: (args, root) => ({ title: 'Edit', subtitle: pathArg(args, root) }),
  Body: ({ args }) => editBody(args),
  ApprovalBody: ({ args }) => editBody(args),
}

const multiEditRenderer: ToolRenderer = {
  label: 'Edit',
  display: (args, root) => ({ title: 'Edit', subtitle: pathArg(args, root) }),
  Body: ({ args }) => multiEditBody(args),
  ApprovalBody: ({ args }) => multiEditBody(args),
}

const writeRenderer: ToolRenderer = {
  label: 'Write',
  display: (args, root) => ({ title: 'Write', subtitle: pathArg(args, root) }),
  Body: ({ args }) => allNewBody(str(args.content)),
  ApprovalBody: ({ args }) => allNewBody(str(args.content)),
}

const notebookEditRenderer: ToolRenderer = {
  label: 'Edit',
  display: (args, root) => ({
    title: 'Edit',
    subtitle: pathArg(args, root, 'notebook_path'),
  }),
  // The SDK carries no old cell source: show the new source as all-new.
  Body: ({ args }) => allNewBody(str(args.new_source)),
  ApprovalBody: ({ args }) => allNewBody(str(args.new_source)),
}

function searchHeader(
  title: string,
  args: Record<string, unknown>,
  root: string | null | undefined,
): ToolHeader {
  const pattern = str(args.pattern)
  const path = str(args.path)
  const where = path === null ? null : shortenPath(path, root)
  return {
    title,
    subtitle:
      pattern === null
        ? where
        : where === null
          ? pattern
          : `${pattern} in ${where}`,
  }
}

const TOOL_RENDERERS: Record<string, ToolRenderer> = {
  Bash: {
    label: 'Command',
    display: (args) => ({
      title: str(args.description) ?? 'Command',
      subtitle: str(args.command),
    }),
    Body: ({ args, output }) => {
      const command = str(args.command)
      if (command === null && output === null) return null
      return (
        <>
          <CommandBlock command={command} />
          <OutputPre output={output} />
        </>
      )
    },
    ApprovalBody: ({ args }) => <CommandBlock command={str(args.command)} />,
  },
  Read: readRenderer,
  NotebookRead: readRenderer,
  Edit: editRenderer,
  MultiEdit: multiEditRenderer,
  Write: writeRenderer,
  NotebookEdit: notebookEditRenderer,
  Grep: {
    label: 'Search',
    display: (args, root) => searchHeader('Search', args, root),
    Body: ({ output }) => (output === null ? null : <OutputPre output={output} />),
  },
  Glob: {
    label: 'Find files',
    display: (args, root) => searchHeader('Find files', args, root),
    Body: ({ output }) => (output === null ? null : <OutputPre output={output} />),
  },
  WebFetch: {
    label: 'Fetch',
    display: (args) => ({ title: 'Fetch', subtitle: str(args.url) }),
    Body: ({ output }) => (output === null ? null : <OutputPre output={output} />),
  },
  WebSearch: {
    label: 'Web search',
    display: (args) => ({ title: 'Web search', subtitle: str(args.query) }),
    Body: ({ output }) => (output === null ? null : <OutputPre output={output} />),
  },
  Task: {
    label: 'Subagent',
    display: (args) => ({
      title: str(args.description) ?? 'Subagent',
      subtitle: str(args.subagent_type),
    }),
    Body: ({ args, output }) => {
      const prompt = str(args.prompt)
      if (prompt === null && output === null) return null
      return (
        <>
          {prompt !== null && (
            <details className="mt-1">
              <summary className="cursor-pointer text-2xs text-muted-foreground select-none">
                Prompt
              </summary>
              <pre className={PRE_CLASS}>{prompt}</pre>
            </details>
          )}
          <OutputPre output={output} />
        </>
      )
    },
  },
  TodoWrite: {
    label: 'Todos',
    display: (args) => {
      const todos = Array.isArray(args.todos) ? args.todos.length : null
      return {
        title: 'Update todos',
        subtitle:
          todos === null ? null : `${todos} item${todos === 1 ? '' : 's'}`,
      }
    },
    Body: ({ args }) =>
      Array.isArray(args.todos) && args.todos.length > 0 ? (
        <TodoChecklist todos={args.todos} />
      ) : null,
  },
}

function fallbackRenderer(name: string): ToolRenderer {
  return {
    label: name,
    unknown: true,
    display: (args) => {
      const first = Object.values(args).find(
        (value) => typeof value === 'string' && value !== '',
      )
      return { title: name, subtitle: typeof first === 'string' ? first : null }
    },
  }
}

export function getToolRenderer(name: string): ToolRenderer {
  return TOOL_RENDERERS[name] ?? fallbackRenderer(name)
}
