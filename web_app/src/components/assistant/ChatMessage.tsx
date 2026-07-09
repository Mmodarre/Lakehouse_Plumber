import ReactMarkdown from 'react-markdown'
import remarkGfm from 'remark-gfm'

// Assistant prose renders as GFM markdown. Raw HTML is DISABLED by
// design — react-markdown skips HTML nodes unless rehype-raw is added,
// and it deliberately is not (model output is untrusted).
export function ChatMessage({
  role,
  text,
}: {
  role: 'user' | 'assistant'
  text: string
}) {
  if (role === 'user') {
    return (
      <div className="ml-8 rounded-lg bg-primary/10 px-3 py-2 text-sm break-words whitespace-pre-wrap text-foreground">
        {text}
      </div>
    )
  }
  return (
    <div className="chat-markdown text-sm text-foreground">
      <ReactMarkdown remarkPlugins={[remarkGfm]}>{text}</ReactMarkdown>
    </div>
  )
}
