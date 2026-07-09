import { useEffect, useState } from 'react'
import {
  Download,
  Loader2,
  RefreshCw,
  Settings2,
  SquarePen,
  X,
} from 'lucide-react'
import { Button } from '../ui/button'
import { LoadingSpinner } from '../common/LoadingSpinner'
import { ChatComposer } from './ChatComposer'
import { ChatThread } from './ChatThread'
import { DaemonGate } from './DaemonGate'
import { FilesChangedChip } from './FilesChangedChip'
import { SetupCard } from './SetupCard'
import {
  isRuntimeReady,
  useAssistantSession,
  useAssistantStatus,
  useExecutorConfig,
  useInstallSkill,
  useNewAssistantSession,
  useStartDaemon,
} from '../../hooks/useAssistant'
import { useAssistantStream } from '../../hooks/useAssistantStream'
import { useAssistantStore } from '../../store/assistantStore'

// ── AssistantPanel — right-dock chat panel (lazy-loaded) ────────
//
// Default export on purpose: `React.lazy` in Layout keeps the markdown
// stack (react-markdown + remark-gfm) out of the eager app chunk.
//
// State switchboard, in gate order (every failure mode is an in-panel
// state — never toast-only):
//   status loading → spinner
//   status unreachable → retry card
//   executor not configured → SetupCard (provider choice comes FIRST —
//     it decides which runtime gate even applies)
//   skill not installed → install card
//   omnigent provider, daemon ladder not green → DaemonGate
//   claude provider, bundled SDK binary missing → ClaudeGate
//   ready → thread + composer

/** Skill pre-gate (the 409 LHP-WEB-002 backstop has its own in-thread card). */
function SkillGate() {
  const install = useInstallSkill()
  return (
    <div className="p-3 text-xs text-muted-foreground">
      <div className="mb-1.5 flex items-center gap-1.5 text-sm font-medium text-foreground">
        <Download className="size-4" aria-hidden="true" />
        Install the LHP skill
      </div>
      <p>
        The assistant needs the packaged LHP skill installed into this
        project (.claude/skills/lhp/).
      </p>
      <Button
        size="sm"
        className="mt-2"
        onClick={() => install.mutate()}
        disabled={install.isPending}
      >
        {install.isPending && <Loader2 className="animate-spin" aria-hidden="true" />}
        Install skill
      </Button>
      {install.isError && (
        <p className="mt-1.5 text-error">
          Install failed: {(install.error as Error).message}
        </p>
      )}
    </div>
  )
}

/** Escape hatch under a runtime gate: the gate must never trap the user
 * on a provider they want to leave. */
function SwitchProviderHint({ onClick }: { onClick: () => void }) {
  return (
    <div className="px-3 pb-3 text-xs text-muted-foreground">
      <Button size="sm" variant="ghost" onClick={onClick}>
        <Settings2 aria-hidden="true" />
        Use a different provider…
      </Button>
    </div>
  )
}

/** Claude-provider runtime gate: the SDK's bundled binary was not found. */
function ClaudeGate({ onRetry }: { onRetry: () => void }) {
  return (
    <div className="p-3 text-xs text-muted-foreground">
      <div className="mb-1.5 flex items-center gap-1.5 text-sm font-medium text-foreground">
        <Download className="size-4" aria-hidden="true" />
        Claude runtime unavailable
      </div>
      <p>
        The Claude runtime bundled with <code>claude-agent-sdk</code> was not
        found. Reinstall the webapp extra in the environment running{' '}
        <code>lhp web</code>:
      </p>
      <pre className="mt-1.5 overflow-x-auto rounded bg-muted p-2 text-2xs text-foreground">
        pip install 'lakehouse-plumber[webapp]'
      </pre>
      <Button size="sm" variant="outline" className="mt-2" onClick={onRetry}>
        <RefreshCw aria-hidden="true" />
        Check again
      </Button>
    </div>
  )
}

export default function AssistantPanel() {
  const setPanelOpen = useAssistantStore((s) => s.setPanelOpen)
  const parts = useAssistantStore((s) => s.parts)
  const streaming = useAssistantStore((s) => s.streaming)
  const statusState = useAssistantStore((s) => s.statusState)
  const failure = useAssistantStore((s) => s.failure)
  const interrupted = useAssistantStore((s) => s.interrupted)

  // The panel is mounted only while open, so the queries are simply
  // enabled; the status query polls at 2s until the runtime is ready.
  const status = useAssistantStatus({ enabled: true })
  const provider = status.data?.provider ?? null
  const ready = isRuntimeReady(status.data)
  const chatReady =
    ready &&
    status.data !== undefined &&
    status.data.skill_installed &&
    status.data.executor_configured

  // Enabled as soon as a config exists (not just when fully ready): the
  // settings card must be reachable FROM the runtime gates, or a stored
  // omnigent config with the daemon down would trap the user there.
  const configured = status.data?.executor_configured === true
  const config = useExecutorConfig({ enabled: configured })
  const stream = useAssistantStream()
  const daemonStart = useStartDaemon()
  const newSession = useNewAssistantSession()

  // Reconfigure-executor card, opened from the gear button. Saving marks
  // the active session stale server-side, so the next message reprovisions.
  const [showSetup, setShowSetup] = useState(false)

  // Rehydrate an existing session's thread after a reload — only into an
  // empty, idle thread (a live conversation is never clobbered).
  const session = useAssistantSession({
    enabled: chatReady && status.data?.active_session != null,
  })
  const snapshot = session.data
  useEffect(() => {
    if (snapshot === undefined) return
    const store = useAssistantStore.getState()
    if (store.parts.length === 0 && !store.streaming) {
      store.hydrateFromSnapshot(snapshot)
    }
  }, [snapshot])

  return (
    <div className="flex h-full min-h-0 flex-col">
      <div className="flex h-9 shrink-0 items-center gap-1 border-b border-border px-2">
        <span className="text-xs font-semibold text-foreground">Assistant</span>
        <FilesChangedChip streaming={streaming} />
        <div className="ml-auto flex items-center">
          {configured && (
            <Button
              variant="ghost"
              size="icon-sm"
              onClick={() => setShowSetup((v) => !v)}
              disabled={streaming}
              aria-label="Assistant settings"
              title="Change provider"
              className="text-muted-foreground"
            >
              <Settings2 aria-hidden="true" />
            </Button>
          )}
          {chatReady && (
            <Button
              variant="ghost"
              size="icon-sm"
              onClick={() => newSession.mutate()}
              disabled={newSession.isPending || streaming}
              aria-label="New session"
              title="New session"
              className="text-muted-foreground"
            >
              <SquarePen aria-hidden="true" />
            </Button>
          )}
          <Button
            variant="ghost"
            size="icon-sm"
            onClick={() => setPanelOpen(false)}
            aria-label="Close assistant panel"
            title="Close assistant panel"
            className="text-muted-foreground"
          >
            <X aria-hidden="true" />
          </Button>
        </div>
      </div>

      {status.isPending ? (
        <LoadingSpinner className="flex-1" />
      ) : status.isError || status.data === undefined ? (
        <div className="p-3 text-xs text-muted-foreground">
          <p className="mb-1.5 text-sm font-medium text-foreground">
            Assistant status unavailable
          </p>
          <p>The assistant endpoints could not be reached.</p>
          <Button
            size="sm"
            variant="outline"
            className="mt-2"
            onClick={() => void status.refetch()}
          >
            <RefreshCw aria-hidden="true" />
            Retry
          </Button>
        </div>
      ) : !status.data.executor_configured ? (
        <SetupCard />
      ) : showSetup ? (
        // Ahead of the runtime gates on purpose: switching provider must be
        // possible while a gate blocks (e.g. omnigent configured, daemon down).
        <SetupCard
          initial={config.data ?? null}
          onDone={() => setShowSetup(false)}
        />
      ) : !status.data.skill_installed ? (
        <SkillGate />
      ) : provider === 'omnigent' && !ready ? (
        <>
          <DaemonGate
            status={status.data}
            onStartDaemon={() => daemonStart.mutate()}
            starting={daemonStart.isPending}
          />
          <SwitchProviderHint onClick={() => setShowSetup(true)} />
        </>
      ) : provider === 'claude_sdk' && !ready ? (
        <>
          <ClaudeGate onRetry={() => void status.refetch()} />
          <SwitchProviderHint onClick={() => setShowSetup(true)} />
        </>
      ) : (
        <>
          <ChatThread
            parts={parts}
            streaming={streaming}
            statusState={statusState}
            failure={failure}
            interrupted={interrupted}
            profile={config.data?.profile ?? null}
          />
          <ChatComposer streaming={streaming} onSend={stream.send} />
        </>
      )}
    </div>
  )
}
