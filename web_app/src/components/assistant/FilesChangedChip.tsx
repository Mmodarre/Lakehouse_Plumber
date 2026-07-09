import { useEffect, useState } from 'react'
import { FileDiff } from 'lucide-react'
import { Badge } from '../ui/badge'
import { createPushSource, FILE_CHANGED_EVENT } from '../../api/push'

// Live "files changed" indicator for the current turn. Subscribes to the
// EXISTING file-changed push event (GET /api/events) while a turn is
// streaming — zero new backend work. The count survives the end of the
// turn (so the user sees what the finished turn touched) and resets when
// the next turn starts.
export function FilesChangedChip({ streaming }: { streaming: boolean }) {
  const [count, setCount] = useState(0)
  // Adjust-state-during-render (not an effect): a streaming edge marks a
  // new turn, which starts its file counter from zero.
  const [prevStreaming, setPrevStreaming] = useState(streaming)
  if (streaming !== prevStreaming) {
    setPrevStreaming(streaming)
    if (streaming) setCount(0)
  }

  useEffect(() => {
    if (!streaming) return

    const source = createPushSource()
    if (source === null) return

    const onFileChanged = (event: Event) => {
      if (!(event instanceof MessageEvent) || typeof event.data !== 'string') {
        return
      }
      let paths: unknown
      try {
        paths = (JSON.parse(event.data) as { paths?: unknown }).paths
      } catch {
        return
      }
      if (Array.isArray(paths)) {
        setCount((prev) => prev + paths.length)
      }
    }

    source.addEventListener(FILE_CHANGED_EVENT, onFileChanged)
    return () => {
      source.close()
    }
  }, [streaming])

  if (count === 0) return null
  return (
    <Badge
      variant="outline"
      className="gap-1 rounded-sm px-1.5 text-2xs text-muted-foreground"
    >
      <FileDiff className="size-3" aria-hidden="true" />
      {count === 1 ? '1 file changed' : `${count} files changed`}
    </Badge>
  )
}
