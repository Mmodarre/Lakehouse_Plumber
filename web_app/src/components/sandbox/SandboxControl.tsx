import { useEffect, useState } from 'react'
import { Beaker } from 'lucide-react'
import { useSandbox } from '../../hooks/useSandbox'
import { useUIStore } from '../../store/uiStore'
import { Switch } from '../ui/switch'
import { SandboxPickerDialog } from './SandboxPickerDialog'
import { cn } from '../../lib/utils'

/**
 * Header control for developer-sandbox mode: a toggle plus, when on, a scope
 * pill. Flipping it on with no profile opens the picker instead of enabling
 * (there is nothing to scope to yet); the pill opens the picker to edit.
 */
export function SandboxControl() {
  const enabled = useUIStore((s) => s.sandboxEnabled)
  const setEnabled = useUIStore((s) => s.setSandboxEnabled)
  const { data } = useSandbox()
  const [pickerOpen, setPickerOpen] = useState(false)

  const profileExists = data?.profile_exists ?? false
  const hasError = Boolean(data?.error)
  const count = data?.resolved_pipelines?.length ?? 0

  const onToggle = (next: boolean) => {
    // Scoping needs a profile; send the developer to the picker to make one.
    if (next && !profileExists) {
      setPickerOpen(true)
      return
    }
    setEnabled(next)
  }

  const pill = pillState(enabled, profileExists, hasError, count)

  // Keep the persisted toggle honest. `sandboxEnabled` survives reloads and
  // project switches in localStorage, so a stale `true` can outlive its
  // profile. Once the scope view has resolved and reports no profile, turn the
  // toggle back off — the invariant `onToggle` enforces on the way in — so a
  // profileless run can't send `--sandbox` (which the backend rejects).
  useEffect(() => {
    if (enabled && data && !data.profile_exists) {
      setEnabled(false)
    }
  }, [enabled, data, setEnabled])

  return (
    <div className="flex items-center gap-1.5">
      <label
        className="flex h-8 cursor-pointer items-center gap-1.5 rounded-md border border-border px-2"
        title="Developer-sandbox mode — generate only your pipelines"
      >
        <Beaker
          className={cn('size-3.5 shrink-0', enabled ? 'text-primary' : 'text-muted-foreground')}
          aria-hidden="true"
        />
        <span
          className={cn(
            'text-2xs font-medium',
            enabled ? 'text-foreground' : 'text-muted-foreground',
          )}
        >
          Sandbox
        </span>
        <Switch
          size="sm"
          checked={enabled}
          onCheckedChange={onToggle}
          aria-label={enabled ? 'Disable sandbox mode' : 'Enable sandbox mode'}
        />
      </label>

      {pill && (
        <button
          type="button"
          onClick={() => setPickerOpen(true)}
          title={hasError ? (data?.error ?? undefined) : 'Edit sandbox scope'}
          className={cn(
            'inline-flex h-6 items-center gap-1 rounded-sm border px-1.5 text-2xs transition-colors',
            pill.className,
          )}
        >
          <Beaker className="size-3 shrink-0" aria-hidden="true" />
          <span className="truncate">{pill.label}</span>
        </button>
      )}

      <SandboxPickerDialog open={pickerOpen} onOpenChange={setPickerOpen} />
    </div>
  )
}

/** The scope pill's label + tint, or null when the toggle is off. */
function pillState(
  enabled: boolean,
  profileExists: boolean,
  hasError: boolean,
  count: number,
): { label: string; className: string } | null {
  if (!enabled) return null
  if (!profileExists) {
    return { label: 'Set up', className: 'border-border text-muted-foreground hover:text-foreground' }
  }
  if (hasError) {
    return { label: 'Scope issue', className: 'border-error/30 bg-error/10 text-error' }
  }
  return {
    label: `${count} ${count === 1 ? 'pipeline' : 'pipelines'}`,
    className: 'border-primary/30 bg-primary/10 text-primary',
  }
}
