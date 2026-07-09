import { useEffect, useRef } from 'react'
import { CircleSlash, Download, Loader2, MessageSquare } from 'lucide-react'
import { Button } from '../ui/button'
import { ApprovalCard } from './ApprovalCard'
import { ChatMessage } from './ChatMessage'
import { ReasoningDisclosure } from './ReasoningDisclosure'
import { SessionFailedCard } from './SessionFailedCard'
import { ToolCallCard } from './ToolCallCard'
import { ToolCallGroup } from './ToolCallGroup'
import { groupParts } from './toolGrouping'
import { useInstallSkill } from '../../hooks/useAssistant'
import type {
  AssistantFailure,
  MessagePart,
} from '../../store/assistantStore'

function Divider({ label }: { label: string }) {
  return (
    <div
      role="separator"
      className="my-1 flex items-center gap-2 text-2xs text-muted-foreground"
    >
      <span className="h-px flex-1 bg-border" />
      {label}
      <span className="h-px flex-1 bg-border" />
    </div>
  )
}

function Part({ part }: { part: MessagePart }) {
  switch (part.kind) {
    case 'text':
      return <ChatMessage role={part.role} text={part.text} />
    case 'reasoning':
      return <ReasoningDisclosure text={part.text} />
    case 'item':
      return <ToolCallCard item={part.item} />
    case 'approval':
      return (
        <ApprovalCard
          elicitationId={part.elicitationId}
          params={part.params}
          resolved={part.resolved}
        />
      )
    case 'divider':
      return <Divider label={part.label} />
  }
}

/** Generic in-panel failure card (the non-session.failed shapes). */
function FailureCard({ failure }: { failure: AssistantFailure }) {
  const install = useInstallSkill()

  if (failure.kind === 'session_failed') return null // rendered separately

  if (failure.kind === 'gate' && failure.code === 'LHP-WEB-002') {
    return (
      <div className="rounded-md border border-warning/50 bg-warning/10 px-2.5 py-2 text-xs">
        <p className="font-medium text-foreground">
          The LHP skill is not installed (or is outdated)
        </p>
        <p className="mt-0.5 text-muted-foreground">
          The assistant needs the packaged LHP skill in this project.
        </p>
        <Button
          size="sm"
          className="mt-1.5"
          onClick={() => install.mutate()}
          disabled={install.isPending}
        >
          {install.isPending ? (
            <Loader2 className="animate-spin" aria-hidden="true" />
          ) : (
            <Download aria-hidden="true" />
          )}
          Install skill
        </Button>
        {install.isError && (
          <p className="mt-1 text-error">
            Install failed: {(install.error as Error).message}
          </p>
        )}
      </div>
    )
  }

  const text =
    failure.kind === 'turn_failed'
      ? `The turn failed: ${failure.reason}`
      : failure.kind === 'stream_error'
        ? `${failure.frame.title} (${failure.frame.code})`
        : failure.kind === 'gate'
          ? failure.message
          : `Connection to the assistant was lost: ${failure.message}`

  const suggestions =
    failure.kind === 'stream_error' ? failure.frame.suggestions : []

  return (
    <div className="rounded-md border border-error/50 bg-error/10 px-2.5 py-2 text-xs">
      <p className="break-words font-medium text-foreground">{text}</p>
      {suggestions.map((s) => (
        <p key={s} className="mt-0.5 text-muted-foreground">
          {s}
        </p>
      ))}
    </div>
  )
}

export function ChatThread({
  parts,
  streaming,
  statusState,
  failure,
  interrupted,
  profile,
}: {
  parts: MessagePart[]
  streaming: boolean
  statusState: string | null
  failure: AssistantFailure | null
  interrupted: boolean
  /** Configured Databricks profile (for session-failed remediation copy). */
  profile: string | null
}) {
  const scrollRef = useRef<HTMLDivElement>(null)

  // Pin the view to the newest content while a turn streams in.
  useEffect(() => {
    const el = scrollRef.current
    if (el !== null) el.scrollTop = el.scrollHeight
  }, [parts, failure, interrupted])

  return (
    <div
      ref={scrollRef}
      className="custom-scrollbar min-h-0 flex-1 overflow-y-auto p-3"
    >
      {parts.length === 0 && !streaming && failure === null && (
        <div className="flex h-full flex-col items-center justify-center text-center text-xs text-muted-foreground">
          <MessageSquare className="mb-2 size-5" aria-hidden="true" />
          Ask about this project — flowgroups, pipelines, generated code.
        </div>
      )}
      <div className="flex flex-col gap-2">
        {groupParts(parts, streaming).map((entry) =>
          entry.kind === 'group' ? (
            <ToolCallGroup key={`group-${entry.id}`} parts={entry.parts} />
          ) : (
            <Part key={entry.part.id} part={entry.part} />
          ),
        )}
        {streaming && (
          <div
            role="status"
            className="flex items-center gap-1.5 text-xs text-muted-foreground"
          >
            <Loader2 className="size-3.5 animate-spin" aria-hidden="true" />
            {statusState === 'preparing' ? 'Preparing session…' : 'Working…'}
          </div>
        )}
        {interrupted && (
          <div className="flex items-center gap-1.5 text-xs text-muted-foreground">
            <CircleSlash className="size-3.5" aria-hidden="true" />
            Interrupted
          </div>
        )}
        {failure !== null &&
          (failure.kind === 'session_failed' ? (
            <SessionFailedCard
              detail={failure.detail}
              hint={failure.hint}
              profile={profile}
            />
          ) : (
            <FailureCard failure={failure} />
          ))}
      </div>
    </div>
  )
}
