// ── designerValidation — map validate issues onto canvas nodes ─
//
// `lhp validate` reports `ValidationIssueView`s that carry a pipeline and
// (usually) a flowgroup, but NO line/column — so an issue cannot be anchored
// to a specific action field by position. We map best-effort by NAME: an
// issue is attributed to the action whose name appears (as a whole token) in
// the issue's title/details. Issues that name no action, or name one that
// isn't on the canvas, stay flowgroup-level (nodeId `null`) and surface only
// in the problems strip. Pure TypeScript: no React, no store, fully testable.

import type { ValidationIssue } from '@/types/api'

export interface NodeCounts {
  errors: number
  warnings: number
}

export interface MappedIssue {
  issue: ValidationIssue
  /** Canvas node id this issue maps to, or `null` when it isn't node-specific. */
  nodeId: string | null
}

export interface DesignerValidation {
  /** Per-node error/warning tallies (only the nodes with ≥1 mapped issue). */
  perNode: Map<string, NodeCounts>
  /** Every issue for this flowgroup, each tagged with its node (or `null`). */
  issues: MappedIssue[]
  errorCount: number
  warningCount: number
}

/** Minimal node view the mapper needs (id + the action's display name). */
export interface ValidationNode {
  id: string
  name: string
}

function isIdentChar(ch: string): boolean {
  return ch !== '' && /[A-Za-z0-9_]/.test(ch)
}

/**
 * True when `name` appears in `text` as a whole identifier token (not as a
 * substring of a longer name), e.g. `orders` matches in "view 'orders' is
 * missing" but not inside `orders_clean`.
 */
export function textMentionsName(text: string, name: string): boolean {
  if (name === '') return false
  let from = 0
  for (;;) {
    const at = text.indexOf(name, from)
    if (at < 0) return false
    const before = at === 0 ? '' : text[at - 1]
    const after = text[at + name.length] ?? ''
    if (!isIdentChar(before) && !isIdentChar(after)) return true
    from = at + 1
  }
}

/** Issues for this flowgroup, plus pipeline-level issues with no flowgroup. */
function belongsToFlowgroup(issue: ValidationIssue, flowgroup: string, pipeline: string): boolean {
  if (issue.flowgroup_name === flowgroup) return true
  return issue.flowgroup_name === null && issue.pipeline_name === pipeline
}

/**
 * Partition validate issues into per-node tallies (for badges) and a tagged
 * flowgroup issue list (for the strip). When several action names are
 * mentioned, the longest (most specific) match wins so badges and strip agree.
 */
export function mapIssuesToNodes(
  issues: readonly ValidationIssue[],
  nodes: readonly ValidationNode[],
  flowgroup: string,
  pipeline: string,
): DesignerValidation {
  const perNode = new Map<string, NodeCounts>()
  const mapped: MappedIssue[] = []
  let errorCount = 0
  let warningCount = 0

  for (const issue of issues) {
    if (!belongsToFlowgroup(issue, flowgroup, pipeline)) continue
    if (issue.severity === 'error') errorCount++
    else warningCount++

    const text = `${issue.title}\n${issue.details ?? ''}`
    let best: ValidationNode | null = null
    for (const node of nodes) {
      if (!textMentionsName(text, node.name)) continue
      if (best === null || node.name.length > best.name.length) best = node
    }

    const nodeId = best?.id ?? null
    mapped.push({ issue, nodeId })
    if (nodeId !== null) {
      const counts = perNode.get(nodeId) ?? { errors: 0, warnings: 0 }
      if (issue.severity === 'error') counts.errors++
      else counts.warnings++
      perNode.set(nodeId, counts)
    }
  }

  return { perNode, issues: mapped, errorCount, warningCount }
}
