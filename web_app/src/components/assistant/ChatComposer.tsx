import { useState } from 'react'
import { Send, Shield, ShieldAlert, ShieldCheck, Square } from 'lucide-react'
import { Button } from '../ui/button'
import { Textarea } from '../ui/textarea'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
} from '../ui/select'
import { useInterruptAssistant } from '../../hooks/useAssistant'
import { useAssistantStore } from '../../store/assistantStore'
import type { PermissionMode } from '../../types/assistant'

// Mirrors Claude Code's permission modes; the backend widens its
// auto-allow policy per mode (claude_sdk provider only — the omnigent
// provider ignores the field and keeps its own elicitation flow).
const PERMISSION_MODES: Array<{
  value: PermissionMode
  label: string
  hint: string
  Icon: typeof Shield
}> = [
  {
    value: 'default',
    label: 'Ask every time',
    hint: 'Every file edit, command, and web access needs your approval.',
    Icon: Shield,
  },
  {
    value: 'acceptEdits',
    label: 'Accept edits',
    hint: 'File edits run without asking; commands still need approval.',
    Icon: ShieldCheck,
  },
  {
    value: 'bypassPermissions',
    label: 'Allow all',
    hint: 'Nothing asks for approval — the assistant runs every tool.',
    Icon: ShieldAlert,
  },
]

function PermissionModeSelect() {
  const mode = useAssistantStore((s) => s.permissionMode)
  const setMode = useAssistantStore((s) => s.setPermissionMode)
  const current =
    PERMISSION_MODES.find((m) => m.value === mode) ?? PERMISSION_MODES[0]

  return (
    <Select value={mode} onValueChange={(v) => setMode(v as PermissionMode)}>
      <SelectTrigger
        size="sm"
        aria-label="Permission mode"
        title={current.hint}
        className="gap-1 border-none bg-transparent px-1.5 text-2xs text-muted-foreground shadow-none hover:text-foreground dark:bg-transparent dark:hover:bg-input/30"
      >
        {/* Manual trigger content: SelectValue would render the selected
            item's children, hint line included. */}
        <current.Icon className="size-3" aria-hidden="true" />
        <span>{current.label}</span>
      </SelectTrigger>
      {/* Popper mode, not the item-aligned default: the trigger sits at the
          bottom of the viewport, and item-aligned content has no collision
          handling — it opens off-screen there. */}
      <SelectContent position="popper" side="top" align="start" className="max-w-[300px]">
        {PERMISSION_MODES.map(({ value, label, hint }) => (
          <SelectItem key={value} value={value}>
            <div className="flex flex-col items-start">
              <span className="text-xs">{label}</span>
              <span className="text-2xs text-muted-foreground">{hint}</span>
            </div>
          </SelectItem>
        ))}
      </SelectContent>
    </Select>
  )
}

export function ChatComposer({
  streaming,
  onSend,
}: {
  streaming: boolean
  onSend: (message: string) => void
}) {
  const [draft, setDraft] = useState('')
  const interrupt = useInterruptAssistant()

  const send = () => {
    const message = draft.trim()
    if (message === '' || streaming) return
    setDraft('')
    onSend(message)
  }

  return (
    <div className="border-t border-border p-2">
      <div className="flex items-end gap-1.5">
        <Textarea
          value={draft}
          onChange={(e) => setDraft(e.target.value)}
          onKeyDown={(e) => {
            if (e.key === 'Enter' && !e.shiftKey) {
              e.preventDefault()
              send()
            }
          }}
          placeholder="Ask the assistant… (Enter to send, Shift+Enter for a new line)"
          aria-label="Chat message"
          className="max-h-40 min-h-9 flex-1 resize-none text-sm"
          rows={1}
        />
        {streaming ? (
          // Stop = POST /interrupt; the stream stays open and the backend
          // ends the turn with an `interrupted` frame.
          <Button
            variant="outline"
            size="icon-sm"
            onClick={() => interrupt.mutate()}
            disabled={interrupt.isPending}
            aria-label="Stop the running turn"
            title="Stop the running turn"
          >
            <Square aria-hidden="true" />
          </Button>
        ) : (
          <Button
            size="icon-sm"
            onClick={send}
            disabled={draft.trim() === ''}
            aria-label="Send message"
            title="Send message"
          >
            <Send aria-hidden="true" />
          </Button>
        )}
      </div>
      {/* Applies from the NEXT sent message; changing it mid-turn does not
          affect the running turn. */}
      <div className="mt-1 flex items-center">
        <PermissionModeSelect />
      </div>
    </div>
  )
}
