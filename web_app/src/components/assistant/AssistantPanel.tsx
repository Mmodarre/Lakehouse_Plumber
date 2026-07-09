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
  isDaemonReady,
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
//   daemon ladder not green → DaemonGate (binary / server / host rungs)
//   skill not installed → install card
//   executor not configured → SetupCard (blocks the composer)
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

export default function AssistantPanel() {
  const setPanelOpen = useAssistantStore((s) => s.setPanelOpen)
  const parts = useAssistantStore((s) => s.parts)
  const streaming = useAssistantStore((s) => s.streaming)
  const statusState = useAssistantStore((s) => s.statusState)
  const failure = useAssistantStore((s) => s.failure)
  const interrupted = useAssistantStore((s) => s.interrupted)

  // The panel is mounted only while open, so the queries are simply
  // enabled; the status query polls at 2s until the daemon is ready.
  const status = useAssistantStatus({ enabled: true })
  const ready = isDaemonReady(status.data)
  const chatReady =
    ready &&
    status.data !== undefined &&
    status.data.skill_installed &&
    status.data.executor_configured

  const config = useExecutorConfig({ enabled: chatReady })
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
          {chatReady && (
            <>
              <Button
                variant="ghost"
                size="icon-sm"
                onClick={() => setShowSetup((v) => !v)}
                disabled={streaming}
                aria-label="Assistant settings"
                title="Change executor"
                className="text-muted-foreground"
              >
                <Settings2 aria-hidden="true" />
              </Button>
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
            </>
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
      ) : !ready ? (
        <DaemonGate
          status={status.data}
          onStartDaemon={() => daemonStart.mutate()}
          starting={daemonStart.isPending}
        />
      ) : !status.data.skill_installed ? (
        <SkillGate />
      ) : !status.data.executor_configured ? (
        <SetupCard />
      ) : showSetup ? (
        <SetupCard
          initial={config.data ?? null}
          onDone={() => setShowSetup(false)}
        />
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
