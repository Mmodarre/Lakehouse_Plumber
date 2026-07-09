import { useEffect, useState } from 'react'
import { Download, Loader2, Play, Terminal } from 'lucide-react'
import { Button } from '../ui/button'
import type { AssistantStatus } from '../../types/assistant'

const OMNIGENT_DOCS_URL = 'https://github.com/omnigent-ai/omnigent'

/** How long the "Start it for me" path may poll before the manual
 * copy-paste fallback is offered alongside it. */
const FALLBACK_AFTER_MS = 15_000

function CommandBlock({ commands }: { commands: string[] }) {
  return (
    <pre className="mt-1.5 overflow-x-auto rounded bg-muted p-2 font-mono text-xs text-foreground">
      {commands.join('\n')}
    </pre>
  )
}

/**
 * Daemon readiness ladder, one rung visible at a time:
 * binary missing → server down → host offline. Every rung is an in-panel
 * state with a remediation path; the status query (polling at 2s while
 * not ready) advances the ladder automatically.
 */
export function DaemonGate({
  status,
  onStartDaemon,
  starting,
}: {
  status: AssistantStatus
  onStartDaemon: () => void
  starting: boolean
}) {
  // After 15s of polling without the ladder going green, also offer the
  // manual commands (the spawn may have failed silently).
  const [showFallback, setShowFallback] = useState(false)
  useEffect(() => {
    const timer = window.setTimeout(() => setShowFallback(true), FALLBACK_AFTER_MS)
    return () => window.clearTimeout(timer)
  }, [])

  // A missing binary only matters while the server is down: a daemon run
  // from a venv (binary off PATH) is a supported setup, and "Start it for
  // me" is the only thing that genuinely needs the binary.
  if (!status.binary_found && !status.server_ok) {
    return (
      <div className="p-3 text-xs text-muted-foreground">
        <div className="mb-1.5 flex items-center gap-1.5 text-sm font-medium text-foreground">
          <Download className="size-4" aria-hidden="true" />
          omnigent is not installed
        </div>
        <p>
          The assistant is powered by the omnigent daemon. Install it once,
          then reopen this panel:
        </p>
        <CommandBlock commands={['uv tool install omnigent']} />
        <p className="mt-1.5">
          <a
            href={OMNIGENT_DOCS_URL}
            target="_blank"
            rel="noreferrer"
            className="text-primary underline underline-offset-2"
          >
            omnigent documentation
          </a>
        </p>
      </div>
    )
  }

  const rung = !status.server_ok
    ? {
        title: 'The omnigent server is not running',
        body: 'The daemon is installed but its local server is not up yet.',
      }
    : {
        title: 'No omnigent host is online',
        body: 'The server is up, but no host has registered to run sessions.',
      }

  return (
    <div className="p-3 text-xs text-muted-foreground">
      <div className="mb-1.5 flex items-center gap-1.5 text-sm font-medium text-foreground">
        <Terminal className="size-4" aria-hidden="true" />
        {rung.title}
      </div>
      <p>{rung.body}</p>
      {status.binary_found && (
        <Button
          size="sm"
          className="mt-2"
          onClick={onStartDaemon}
          disabled={starting}
        >
          {starting ? (
            <Loader2 className="animate-spin" aria-hidden="true" />
          ) : (
            <Play aria-hidden="true" />
          )}
          Start it for me
        </Button>
      )}
      {starting && (
        <p className="mt-1.5" role="status">
          Starting the daemon — this panel updates as soon as it is ready…
        </p>
      )}
      {(showFallback || !status.binary_found) && (
        <div className="mt-2">
          <p>Not coming up? Run it manually in a terminal:</p>
          <CommandBlock commands={['omnigent server start', 'omnigent host']} />
        </div>
      )}
    </div>
  )
}
