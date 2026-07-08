import { useState } from 'react'

function JsonValue({ value, depth }: { value: unknown; depth: number }) {
  const [expanded, setExpanded] = useState(depth < 2)

  if (value === null) {
    return <span className="text-muted-foreground/70">null</span>
  }

  if (typeof value === 'boolean') {
    return <span className="text-warning">{String(value)}</span>
  }

  if (typeof value === 'number') {
    return <span className="text-info tabular-nums">{value}</span>
  }

  if (typeof value === 'string') {
    return <span className="text-success">"{value}"</span>
  }

  if (Array.isArray(value)) {
    if (value.length === 0) return <span className="text-muted-foreground/70">[]</span>
    return (
      <div>
        <button
          aria-expanded={expanded}
          className="text-muted-foreground transition-colors hover:text-foreground"
          onClick={() => setExpanded(!expanded)}
        >
          {expanded ? '[-]' : `[${value.length}]`}
        </button>
        {expanded && (
          <div className="ml-3 border-l border-border/60 pl-2">
            {value.map((item, i) => (
              <div key={i}>
                <JsonValue value={item} depth={depth + 1} />
              </div>
            ))}
          </div>
        )}
      </div>
    )
  }

  if (typeof value === 'object') {
    const entries = Object.entries(value as Record<string, unknown>)
    if (entries.length === 0) return <span className="text-muted-foreground/70">{'{}'}</span>
    return (
      <div>
        <button
          aria-expanded={expanded}
          className="text-muted-foreground transition-colors hover:text-foreground"
          onClick={() => setExpanded(!expanded)}
        >
          {expanded ? '{-}' : `{${entries.length}}`}
        </button>
        {expanded && (
          <div className="ml-3 border-l border-border/60 pl-2">
            {entries.map(([key, val]) => (
              <div key={key}>
                <span className="font-medium text-foreground">{key}</span>
                <span className="text-muted-foreground">: </span>
                <JsonValue value={val} depth={depth + 1} />
              </div>
            ))}
          </div>
        )}
      </div>
    )
  }

  return <span className="text-muted-foreground">{String(value)}</span>
}

export function JsonTree({ data }: { data: unknown }) {
  return (
    <div className="rounded-md bg-muted/50 p-2 font-mono text-2xs">
      <JsonValue value={data} depth={0} />
    </div>
  )
}
