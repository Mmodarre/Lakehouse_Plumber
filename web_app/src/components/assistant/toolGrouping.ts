import type { MessagePart } from '../../store/assistantConversation'

// View-layer grouping of tool-call runs — the store keeps flat parts.

/** Runs of at least this many consecutive completed tool calls collapse. */
export const GROUP_MIN = 3

export type ThreadEntry =
  | { kind: 'single'; part: MessagePart }
  | { kind: 'group'; id: number; parts: MessagePart[] }

/** Only settled, successful tool calls collapse: running/incomplete parts
 * must stay visible, and a failed call is load-bearing context. */
function isCollapsible(part: MessagePart): boolean {
  return (
    part.kind === 'item' &&
    (part.item.type === 'tool_call' || part.item.type === 'function_call') &&
    part.item.status === 'completed'
  )
}

/** Collapse runs of ≥ {@link GROUP_MIN} consecutive completed tool calls
 * into group entries. The TRAILING run stays expanded while `streaming` —
 * live activity is always visible. */
export function groupParts(
  parts: MessagePart[],
  streaming: boolean,
): ThreadEntry[] {
  const entries: ThreadEntry[] = []
  let run: MessagePart[] = []

  const flush = (trailing: boolean) => {
    if (run.length >= GROUP_MIN && !(trailing && streaming)) {
      entries.push({ kind: 'group', id: run[0].id, parts: run })
    } else {
      for (const part of run) entries.push({ kind: 'single', part })
    }
    run = []
  }

  for (const part of parts) {
    if (isCollapsible(part)) {
      run.push(part)
      continue
    }
    flush(false)
    entries.push({ kind: 'single', part })
  }
  flush(true)
  return entries
}
