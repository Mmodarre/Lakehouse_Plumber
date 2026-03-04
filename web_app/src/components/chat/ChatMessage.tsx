/** Single chat message bubble (user or assistant). */

import ReactMarkdown from 'react-markdown'
import remarkGfm from 'remark-gfm'
import { ToolCallCard } from './ToolCallCard'
import { QuestionCard, isQuestionTool } from './QuestionCard'
import { TodoCard, isTodoTool } from './TodoCard'
import { StreamingIndicator } from './StreamingIndicator'
import type { ChatMessage as ChatMessageType } from '../../types/chat'

export function ChatMessage({ message }: { message: ChatMessageType }) {
  const isUser = message.role === 'user'

  return (
    <div className={`flex ${isUser ? 'justify-end' : 'justify-start'} px-3 py-1`}>
      <div
        className={`max-w-[85%] rounded-lg px-3 py-2 text-[12px] leading-relaxed ${
          isUser
            ? 'bg-blue-50 text-slate-800'
            : 'bg-white text-slate-700 shadow-sm ring-1 ring-slate-200'
        }`}
      >
        {message.parts.map((part, i) => {
          if (part.type === 'text' && part.text) {
            if (isUser) {
              // User messages: plain text
              return (
                <div key={i} className="whitespace-pre-wrap break-words">
                  {part.text}
                </div>
              )
            }
            // Assistant messages: render markdown
            return (
              <div key={i} className="chat-markdown break-words">
                <ReactMarkdown remarkPlugins={[remarkGfm]}>
                  {part.text}
                </ReactMarkdown>
              </div>
            )
          }
          if (part.type === 'reasoning' && part.text) {
            return (
              <details key={i} className="my-1 rounded border border-slate-200 bg-slate-50">
                <summary className="cursor-pointer select-none px-2 py-1 text-[10px] font-medium text-slate-500">
                  Thinking
                </summary>
                <div className="border-t border-slate-200 px-2 py-1.5 text-[11px] text-slate-500 whitespace-pre-wrap">
                  {part.text}
                </div>
              </details>
            )
          }
          if (part.type === 'tool-call') {
            if (isQuestionTool(part.toolName)) {
              return <QuestionCard key={i} part={part} />
            }
            if (isTodoTool(part.toolName)) {
              return <TodoCard key={i} part={part} />
            }
            return <ToolCallCard key={i} part={part} />
          }
          return null
        })}

        {message.isStreaming && (
          <StreamingIndicator />
        )}
      </div>
    </div>
  )
}
