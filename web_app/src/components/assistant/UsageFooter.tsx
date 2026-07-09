import type { ReactNode } from 'react'
import type { ExecutorMode, UsageTotals } from '../../types/assistant'

// ── UsageFooter — compact session token/cost line ───────────────
//
// Rendered between the thread and the composer. The backend supplies the
// numbers (session_totals on terminal frames / usage_totals on snapshots);
// this component only labels them per auth mode:
//   claude_subscription → the SDK's own estimate, marked "est."
//   databricks          → the SDK number is meaningless for billing: show
//                         the configured-pricing cost when the backend
//                         computed one, else a "Set pricing" link.

function formatTokens(n: number): string {
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`
  if (n >= 1_000) return `${(n / 1_000).toFixed(1)}k`
  return String(n)
}

function formatUsd(n: number): string {
  return n > 0 && n < 0.005 ? '<$0.01' : `$${n.toFixed(2)}`
}

export function UsageFooter({
  usage,
  mode,
  onSetPricing,
}: {
  usage: UsageTotals | null
  mode: ExecutorMode | null
  onSetPricing: () => void
}) {
  if (usage === null) return null
  const input = usage.input_tokens ?? 0
  const output = usage.output_tokens ?? 0
  const cacheRead = usage.cache_read_input_tokens ?? 0
  const cacheWrite = usage.cache_creation_input_tokens ?? 0
  const cache = cacheRead + cacheWrite
  if (input + output + cache === 0) return null

  const segments = [
    `${formatTokens(input)} in`,
    `${formatTokens(output)} out`,
    ...(cache > 0 ? [`${formatTokens(cache)} cache`] : []),
  ]

  let cost: ReactNode = null
  if (mode === 'claude_subscription' && usage.sdk_cost_usd != null) {
    cost = <span>{formatUsd(usage.sdk_cost_usd)} est.</span>
  } else if (mode === 'databricks') {
    cost =
      usage.configured_cost_usd != null ? (
        <span>{formatUsd(usage.configured_cost_usd)}</span>
      ) : (
        <button
          type="button"
          onClick={onSetPricing}
          className="underline decoration-dotted underline-offset-2 hover:text-foreground"
        >
          Set pricing
        </button>
      )
  }

  const rawCounts =
    `input ${input} · output ${output} · ` +
    `cache read ${cacheRead} · cache write ${cacheWrite}`

  return (
    <div
      className="flex shrink-0 items-center gap-1 border-t border-border px-3 py-1 text-2xs text-muted-foreground"
      title={rawCounts}
      data-testid="usage-footer"
    >
      <span>{segments.join(' · ')}</span>
      {cost !== null && (
        <>
          <span aria-hidden="true">·</span>
          {cost}
        </>
      )}
    </div>
  )
}
