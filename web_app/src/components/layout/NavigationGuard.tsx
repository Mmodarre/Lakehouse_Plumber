import { useCallback, useRef } from 'react'
import { useBlocker } from 'react-router-dom'
import { DiscardChangesDialog } from '../editor/DiscardChangesDialog'
import { useDirtyGuardStore } from '../../store/dirtyGuardStore'

// ── NavigationGuard — the single app-level route blocker ─────
//
// Mounted ONCE by Layout (inside the data-router context). Owns the app's
// only useBlocker registration and prompts over every dirtyGuardStore
// source that blocks the attempted navigation; confirming runs each
// blocking source's onDiscard, then lets the navigation proceed.

export function NavigationGuard() {
  const sources = useDirtyGuardStore((s) => s.sources)
  const decidedRef = useRef(false)

  const blocker = useBlocker(
    useCallback(
      ({ nextLocation }: { nextLocation: { pathname: string } }) =>
        Object.values(sources).some(
          (source) => source.blocks?.(nextLocation.pathname) ?? true,
        ),
      [sources],
    ),
  )

  // Which sources object to THIS navigation (a /config-internal nav must
  // not surface the config form's message even if another source blocked).
  const nextPathname = blocker.location?.pathname
  const blocking =
    blocker.state === 'blocked' && nextPathname !== undefined
      ? Object.values(sources).filter((source) => source.blocks?.(nextPathname) ?? true)
      : []

  return (
    <DiscardChangesDialog
      open={blocker.state === 'blocked'}
      onOpenChange={(open) => {
        if (open) return
        // Radix also fires close after the discard action ran — don't
        // reset a blocker that has already proceeded.
        if (!decidedRef.current && blocker.state === 'blocked') blocker.reset()
        decidedRef.current = false
      }}
      description={blocking.map((source) => source.message).join(' ')}
      onDiscard={() => {
        decidedRef.current = true
        for (const source of blocking) source.onDiscard()
        if (blocker.state === 'blocked') blocker.proceed()
      }}
    />
  )
}
