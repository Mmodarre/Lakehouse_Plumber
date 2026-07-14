import type { ReactNode } from 'react'
import { orderedKinds, KIND_DOT_CLASS } from './explorerData'
import type { MapSeverity } from '../../../hooks/useMapEnrichment'
import { cn } from '@/lib/utils'

// ── explorerPrimitives — shared explorer chrome (T1.3) ───────
//
// The 11px uppercase section micro-label (§4 geometry/type) and the flowgroup
// action-kind dots, shared across the Structure and Tables lenses.

/** 11px uppercase-tracked section micro-label (§4 floor). */
export function SectionHead({ children }: { children: ReactNode }) {
  return (
    <div className="px-3 pt-2.5 pb-1 text-2xs font-bold tracking-[0.11em] text-faint uppercase select-none">
      {children}
    </div>
  )
}

/**
 * Validation-severity dot for an explorer row (error=red, warning=amber),
 * derived by useMapEnrichment from runStore.issues. Renders nothing when the
 * row has no attributable issue.
 */
export function SeverityDot({ severity }: { severity: MapSeverity | undefined }) {
  if (!severity) return null
  return (
    <span
      title={severity}
      aria-label={`${severity} severity`}
      className={cn(
        'block size-[6px] shrink-0 rounded-full',
        severity === 'error' ? 'bg-error' : 'bg-warning',
      )}
    />
  )
}

/**
 * Action-kind dots for a flowgroup row (colored via the `--kind-*` tokens).
 * Kinds come from the flowgroup summary's `action_types`; an unknown kind
 * renders as a neutral dot.
 */
export function KindDots({ actionTypes }: { actionTypes: readonly string[] }) {
  const kinds = orderedKinds(actionTypes)
  if (kinds.length === 0) return null
  return (
    <span className="ml-auto inline-flex shrink-0 items-center gap-[3px] pl-1" aria-hidden="true">
      {kinds.map((kind) => (
        <span
          key={kind}
          title={kind}
          className={cn('block size-[5px] rounded-[1px]', KIND_DOT_CLASS[kind] ?? 'bg-faint')}
        />
      ))}
    </span>
  )
}
