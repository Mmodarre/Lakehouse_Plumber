import { useEffect } from 'react'
import { create } from 'zustand'

// ── dirtyGuardStore — the app's unsaved-changes registry ─────
//
// react-router's data router supports ONE useBlocker registration at a
// time, so every surface with discardable unsaved state (workspace editor
// buffers, config forms, ...) contributes a source here instead of
// registering its own blocker. Layout mounts <NavigationGuard /> once; it
// owns the single useBlocker and prompts over ALL sources that block the
// attempted navigation. This replaces the Task-7 arrangement where the
// config page's route guard had to unmount whenever workspace buffers
// were open (leaving dirty config forms unguarded on in-app navigation).

export interface DirtyGuardSource {
  /** One sentence for the discard prompt (e.g. "Unsaved changes in 2 file(s) will be lost."). */
  message: string
  /**
   * Should navigating to `nextPathname` be blocked? Omit to block every
   * navigation. Lets a surface exempt its own sub-routes (the config form
   * guards tab switches itself, so /config-internal navs pass through).
   */
  blocks?: (nextPathname: string) => boolean
  /** Discard this source's unsaved state (runs before navigation proceeds). */
  onDiscard: () => void
}

interface DirtyGuardState {
  sources: Record<string, DirtyGuardSource>
  register: (id: string, source: DirtyGuardSource) => void
  unregister: (id: string) => void
}

export const useDirtyGuardStore = create<DirtyGuardState>()((set) => ({
  sources: {},
  register: (id, source) =>
    set((s) => ({ sources: { ...s.sources, [id]: source } })),
  unregister: (id) =>
    set((s) => {
      if (!(id in s.sources)) return s
      const next = { ...s.sources }
      delete next[id]
      return { sources: next }
    }),
}))

/**
 * Keep one dirty-guard source registered while `source` is non-null.
 *
 * Pass `null` while clean. The source object is an effect dependency, so
 * callers must memoize it (useMemo) — otherwise it re-registers on every
 * render (harmless, but churns the store).
 */
export function useDirtyGuardSource(id: string, source: DirtyGuardSource | null): void {
  const register = useDirtyGuardStore((s) => s.register)
  const unregister = useDirtyGuardStore((s) => s.unregister)
  useEffect(() => {
    if (source === null) return
    register(id, source)
    return () => unregister(id)
  }, [id, source, register, unregister])
}
