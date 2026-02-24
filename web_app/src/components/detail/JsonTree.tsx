import { useState } from 'react'

function JsonValue({ value, depth }: { value: unknown; depth: number }) {
  const [expanded, setExpanded] = useState(depth < 2)

  if (value === null) {
    return <span className="text-slate-400">null</span>
  }

  if (typeof value === 'boolean') {
    return <span className="text-amber-600">{String(value)}</span>
  }

  if (typeof value === 'number') {
    return <span className="text-blue-600">{value}</span>
  }

  if (typeof value === 'string') {
    return <span className="text-green-700">"{value}"</span>
  }

  if (Array.isArray(value)) {
    if (value.length === 0) return <span className="text-slate-400">[]</span>
    return (
      <div>
        <button
          className="text-slate-400 hover:text-slate-600"
          onClick={() => setExpanded(!expanded)}
        >
          {expanded ? '[-]' : `[${value.length}]`}
        </button>
        {expanded && (
          <div className="ml-3 border-l border-slate-200 pl-2">
            {value.map((item, i) => (
              <div key={i} className="text-[11px]">
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
    if (entries.length === 0) return <span className="text-slate-400">{'{}'}</span>
    return (
      <div>
        <button
          className="text-slate-400 hover:text-slate-600"
          onClick={() => setExpanded(!expanded)}
        >
          {expanded ? '{-}' : `{${entries.length}}`}
        </button>
        {expanded && (
          <div className="ml-3 border-l border-slate-200 pl-2">
            {entries.map(([key, val]) => (
              <div key={key} className="text-[11px]">
                <span className="font-medium text-slate-600">{key}</span>
                <span className="text-slate-400">: </span>
                <JsonValue value={val} depth={depth + 1} />
              </div>
            ))}
          </div>
        )}
      </div>
    )
  }

  return <span className="text-slate-500">{String(value)}</span>
}

export function JsonTree({ data }: { data: unknown }) {
  return (
    <div className="rounded bg-slate-50 p-2 text-[11px] font-mono">
      <JsonValue value={data} depth={0} />
    </div>
  )
}
