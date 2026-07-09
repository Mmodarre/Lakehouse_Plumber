import type { AssistantItem } from '../../types/assistant'

// Argument helpers shared by the per-tool renderer registry
// (`toolRenderers.tsx`), ToolCallCard and ApprovalCard.

/** Parse the arguments of a `tool_call` item: `item.done` carries a JSON
 * string, `item.started` the already-decoded object. */
export function parseToolArguments(item: AssistantItem): Record<string, unknown> {
  const raw = item.arguments
  if (raw !== null && typeof raw === 'object' && !Array.isArray(raw)) {
    return raw as Record<string, unknown>
  }
  if (typeof raw !== 'string') return {}
  try {
    const parsed: unknown = JSON.parse(raw)
    return parsed !== null && typeof parsed === 'object'
      ? (parsed as Record<string, unknown>)
      : {}
  } catch {
    return {}
  }
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
