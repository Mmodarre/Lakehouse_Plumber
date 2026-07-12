import { useMemo } from 'react'
import { useSandbox } from '../../hooks/useSandbox'
import { useUIStore } from '../../store/uiStore'
import { resolveScope, type Scope } from './scopeFilter'

/**
 * The active pipeline scope for filtering the dashboard / lists / file tree,
 * or `null` when nothing should be filtered (toggle OFF, no profile, or an
 * unresolved scope). Reads the persisted toggle and the shared ['sandbox']
 * query, so every consumer sees one consistent scope.
 */
export function useSandboxScope(): Scope {
  const enabled = useUIStore((s) => s.sandboxEnabled)
  const { data } = useSandbox()
  return useMemo(() => resolveScope(enabled, data), [enabled, data])
}
