import type { SchemaPath } from './schema-help'

export interface HelpEntry {
  id: string
  summary: string
  details?: string[]
  choices?: { value: string; explanation: string }[]
  examples?: { label: string; yaml: string }[]
  unsetBehavior?: string
  constraints?: string[]
  relatedHelpIds?: string[]
  sources: { file: string; anchor?: string }[]
  bindings: { path: (string | number)[]; subtype?: string }[]
}
export interface HelpCatalog { version: 1; entries: HelpEntry[] }
export type ResolvedHelp = Pick<HelpEntry, 'summary'> & Partial<Omit<HelpEntry, 'summary'>>

/** Exact subtype and literal path segments win over generic/wildcard bindings. */
export function resolveFieldHelp(catalog: HelpCatalog, path: SchemaPath, subtype?: string): HelpEntry | undefined {
  let best: HelpEntry | undefined
  let bestScore = -1
  for (const entry of catalog.entries) {
    for (const binding of entry.bindings) {
      if (binding.subtype !== undefined && binding.subtype !== subtype) continue
      if (binding.path.length !== path.length) continue
      if (!binding.path.every((part, i) => part === '*' || part === path[i])) continue
      const score = (binding.subtype ? 1000 : 0) + binding.path.filter((part) => part !== '*').length
      if (score > bestScore) { best = entry; bestScore = score }
    }
  }
  return best
}

export function helpSourceLink(source: { file: string; anchor?: string }): { href: string; title: string } {
  const anchor = source.anchor ? `#${encodeURIComponent(source.anchor)}` : ''
  const title = source.file.split('/').at(-1)?.replace(/\.(rst|md)$/, '').replace(/[-_]/g, ' ') ?? 'Documentation'
  if (source.file.startsWith('docs/') && source.file.endsWith('.rst')) {
    return { href: `https://lakehouse-plumber.readthedocs.io/en/latest/${source.file.slice(5, -4)}.html${anchor}`, title }
  }
  const path = source.file.split('/').map(encodeURIComponent).join('/')
  return { href: `https://github.com/Mmodarre/Lakehouse_Plumber/blob/release/V0.9.2/${path}${anchor}`, title }
}
