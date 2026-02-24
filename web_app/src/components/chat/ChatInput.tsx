/** Auto-resizing chat input with send/stop buttons. */

import { useCallback, useRef, useState, type KeyboardEvent } from 'react'
import { ContextBadge } from './ContextBadge'
import type { ChatContext } from '../../types/chat'

export function ChatInput({
  onSend,
  onStop,
  isStreaming,
  context,
  disabled,
}: {
  onSend: (text: string) => void
  onStop: () => void
  isStreaming: boolean
  context: ChatContext
  disabled: boolean
}) {
  const [text, setText] = useState('')
  const textareaRef = useRef<HTMLTextAreaElement>(null)

  const handleSend = useCallback(() => {
    const trimmed = text.trim()
    if (!trimmed || disabled) return
    onSend(trimmed)
    setText('')
    // Reset textarea height
    if (textareaRef.current) {
      textareaRef.current.style.height = 'auto'
    }
  }, [text, disabled, onSend])

  const handleKeyDown = useCallback(
    (e: KeyboardEvent<HTMLTextAreaElement>) => {
      if (e.key === 'Enter' && !e.shiftKey) {
        e.preventDefault()
        if (isStreaming) return
        handleSend()
      }
    },
    [handleSend, isStreaming],
  )

  const handleInput = useCallback(() => {
    const el = textareaRef.current
    if (!el) return
    el.style.height = 'auto'
    el.style.height = `${Math.min(el.scrollHeight, 120)}px`
  }, [])

  return (
    <div className="border-t border-slate-200 bg-white p-2">
      <ContextBadge context={context} />

      <div className="mt-1 flex items-end gap-1.5">
        <textarea
          ref={textareaRef}
          value={text}
          onChange={(e) => {
            setText(e.target.value)
            handleInput()
          }}
          onKeyDown={handleKeyDown}
          placeholder={disabled ? 'AI not available' : 'Ask about your pipeline config...'}
          disabled={disabled}
          rows={1}
          className="max-h-[120px] min-h-[32px] flex-1 resize-none rounded-md border border-slate-200 bg-slate-50 px-2.5 py-1.5 text-[12px] text-slate-700 placeholder:text-slate-400 focus:border-blue-400 focus:bg-white focus:outline-none disabled:opacity-50"
        />

        {isStreaming ? (
          <button
            onClick={onStop}
            className="shrink-0 rounded-md bg-red-50 p-1.5 text-red-600 hover:bg-red-100"
            title="Stop generation (Cmd+.)"
          >
            <svg className="h-4 w-4" fill="currentColor" viewBox="0 0 24 24">
              <rect x="6" y="6" width="12" height="12" rx="1" />
            </svg>
          </button>
        ) : (
          <button
            onClick={handleSend}
            disabled={disabled || !text.trim()}
            className="shrink-0 rounded-md bg-slate-800 p-1.5 text-white hover:bg-slate-700 disabled:opacity-30"
            title="Send (Enter)"
          >
            <svg className="h-4 w-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
              <path strokeLinecap="round" strokeLinejoin="round" d="M12 19V5m0 0l-7 7m7-7l7 7" />
            </svg>
          </button>
        )}
      </div>
    </div>
  )
}
