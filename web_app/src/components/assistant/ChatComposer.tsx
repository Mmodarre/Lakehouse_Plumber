import { useState } from 'react'
import { Send, Square } from 'lucide-react'
import { Button } from '../ui/button'
import { Textarea } from '../ui/textarea'
import { useInterruptAssistant } from '../../hooks/useAssistant'

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
    <div className="flex items-end gap-1.5 border-t border-border p-2">
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
  )
}
