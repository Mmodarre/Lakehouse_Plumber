import { Beaker } from 'lucide-react'
import { useRunStore } from '../../store/runStore'

/**
 * Marks the live run view when the current run was launched in
 * developer-sandbox mode. Renders nothing otherwise, so it is safe to mount
 * unconditionally. (Run history has no persisted sandbox flag, so the badge
 * reflects the in-session run only.)
 */
export function SandboxRunBadge() {
  const sandbox = useRunStore((s) => s.sandbox)
  const runKind = useRunStore((s) => s.runKind)
  if (!sandbox || runKind === null) return null
  return (
    <span
      title="Scope and namespace from .lhp/profile.yaml"
      className="inline-flex items-center gap-1 rounded-sm border border-primary/25 bg-primary/12 px-1.5 py-0.5 text-2xs font-medium text-primary"
    >
      <Beaker className="size-3 shrink-0" aria-hidden="true" />
      Sandbox
    </span>
  )
}
