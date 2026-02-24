/** Scrollable list of chat messages with auto-scroll to bottom. */

import { useEffect, useRef } from 'react'
import { ChatMessage } from './ChatMessage'
import { ChatEmptyState } from './ChatEmptyState'
import type { ChatMessage as ChatMessageType } from '../../types/chat'

export function ChatMessageList({
  messages,
  onSuggestion,
}: {
  messages: ChatMessageType[]
  onSuggestion: (text: string) => void
}) {
  const bottomRef = useRef<HTMLDivElement>(null)

  // Auto-scroll when new messages arrive or content updates
  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: 'smooth' })
  }, [messages])

  if (messages.length === 0) {
    return <ChatEmptyState onSuggestion={onSuggestion} />
  }

  return (
    <div className="flex-1 overflow-y-auto py-2">
      {messages.map((msg) => (
        <ChatMessage key={msg.id} message={msg} />
      ))}
      <div ref={bottomRef} />
    </div>
  )
}
