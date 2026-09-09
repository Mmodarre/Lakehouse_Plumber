import { useEffect, useState } from 'react'
import { loadHelpCached } from '@/api/help'
import type { SchemaKind } from '@/api/schemas'
import { helpSourceLink, type HelpEntry, type ResolvedHelp } from '@/lib/field-help'

export default function FieldHelpBody({ help }: { help: ResolvedHelp }) {
  const [copyStatus, setCopyStatus] = useState('')
  return <div className="mt-3 space-y-3">
    {help.details?.map((paragraph, index) => <p key={index} className="text-muted-foreground">{paragraph}</p>)}
    {help.choices && <dl className="space-y-2">{help.choices.map((choice) => <div key={choice.value}><dt className="font-mono text-xs font-medium">{choice.value}</dt><dd className="text-muted-foreground">{choice.explanation}</dd></div>)}</dl>}
    {help.unsetBehavior && <div><h3 className="font-medium">When unset</h3><p className="text-muted-foreground">{help.unsetBehavior}</p></div>}
    {help.constraints && <ul className="list-disc space-y-1 pl-4 text-muted-foreground">{help.constraints.map((constraint, index) => <li key={index}>{constraint}</li>)}</ul>}
    {help.examples?.map((example, index) => <div key={index}>
      <div className="mb-1 flex items-center justify-between gap-2"><h3 className="text-xs font-medium">{example.label}</h3><button type="button" className="rounded px-2 py-1 text-xs text-primary hover:underline focus-visible:outline-2" onClick={() => {
        void (navigator.clipboard?.writeText(example.yaml) ?? Promise.reject(new Error('Clipboard unavailable'))).then(() => setCopyStatus('Example copied.')).catch(() => setCopyStatus('Copy unavailable. Select and copy the example text.'))
      }}>Copy example</button></div>
      <pre className="select-text overflow-x-auto rounded bg-muted p-3 font-mono text-xs whitespace-pre-wrap break-words">{example.yaml}</pre>
    </div>)}
    {copyStatus && <p role="status" className="text-xs text-muted-foreground">{copyStatus}</p>}
    {!!help.relatedHelpIds?.length && <RelatedGuidance ids={help.relatedHelpIds} />}
    {!!help.sources?.length && <div className="border-t pt-3"><h3 className="mb-1 text-xs font-medium">Documentation</h3><ul className="space-y-1">{help.sources.map((source) => {
      const link = helpSourceLink(source)
      return <li key={source.file + (source.anchor ?? '')}><a href={link.href} target="_blank" rel="noreferrer" className="text-primary underline underline-offset-2">{link.title}<span className="sr-only"> (opens in a new tab)</span></a></li>
    })}</ul></div>}
  </div>
}

/** Cross-category guidance is fetched only after opening this lazy help body. */
function RelatedGuidance({ ids }: { ids: string[] }) {
  const [entries, setEntries] = useState<HelpEntry[]>([])
  useEffect(() => {
    let current = true
    const kinds: Record<string, SchemaKind> = { project: 'project', pipeline: 'pipeline_config', job: 'job_config', flowgroup: 'flowgroup', action: 'flowgroup', template: 'template' }
    void Promise.all(ids.map(async (id) => {
      const kind = kinds[id.split('.')[0]]
      if (!kind) return undefined
      try { return (await loadHelpCached(kind)).entries.find(entry => entry.id === id) }
      catch { return undefined }
    })).then(found => { if (current) setEntries(found.filter((entry): entry is HelpEntry => !!entry)) })
    return () => { current = false }
  }, [ids])
  if (!entries.length) return null
  return <div className="border-t pt-3"><h3 className="mb-1 text-xs font-medium">Related guidance</h3>{entries.map(entry => <details key={entry.id} className="py-1">
    <summary className="cursor-pointer text-primary">{entry.summary}</summary>
    <div className="mt-2 space-y-2 text-muted-foreground">
      {entry.details?.map((text, index) => <p key={index}>{text}</p>)}
      {entry.unsetBehavior && <p>When unset: {entry.unsetBehavior}</p>}
      {entry.sources.map(source => { const link = helpSourceLink(source); return <a key={link.href} href={link.href} target="_blank" rel="noreferrer" className="block text-primary underline underline-offset-2">{link.title}<span className="sr-only"> (opens in a new tab)</span></a> })}
    </div>
  </details>)}</div>
}
