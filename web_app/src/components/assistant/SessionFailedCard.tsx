import { AlertTriangle } from 'lucide-react'
import type { SessionFailedHint } from '../../types/assistant'

/** Hint-driven remediation copy for a `session.failed` frame. */
function remediation(hint: SessionFailedHint, profile: string | null): string {
  switch (hint) {
    case 'omnigent_setup':
      return 'Run `omnigent setup` in a terminal, then try again.'
    case 'databricks_auth':
      return `Run \`databricks auth login --profile ${profile ?? '<profile>'}\` in a terminal, then try again.`
    case 'claude_auth':
      return 'Sign in with `claude` (Claude Code) on this machine — or export the configured token variable (see `claude setup-token`) before starting `lhp web` — then try again.'
    case 'claude_setup':
      return "Reinstall the webapp extra in the environment running `lhp web`: pip install 'lakehouse-plumber[webapp]'."
    default:
      return ''
  }
}

export function SessionFailedCard({
  detail,
  hint,
  profile = null,
}: {
  detail: string
  hint: SessionFailedHint
  /** Configured Databricks CLI profile, for the databricks_auth copy. */
  profile?: string | null
}) {
  const fix = remediation(hint, profile)
  return (
    <div className="rounded-md border border-error/50 bg-error/10 px-2.5 py-2 text-xs">
      <div className="flex items-start gap-1.5">
        <AlertTriangle
          className="mt-0.5 size-3.5 shrink-0 text-error"
          aria-hidden="true"
        />
        <div className="min-w-0 flex-1">
          <p className="font-medium text-foreground">Assistant session failed</p>
          {fix !== '' ? (
            <p className="mt-0.5 text-muted-foreground">{fix}</p>
          ) : (
            <p className="mt-0.5 break-words text-muted-foreground">{detail}</p>
          )}
          {fix !== '' && detail !== '' && (
            <details className="mt-1">
              <summary className="cursor-pointer text-2xs text-muted-foreground select-none">
                Details
              </summary>
              <p className="mt-0.5 break-words text-muted-foreground">{detail}</p>
            </details>
          )}
        </div>
      </div>
    </div>
  )
}
