import type * as Monaco from 'monaco-editor'
import { loadHelpCached } from '../api/help'
import { helpSourceLink, resolveFieldHelp } from './field-help'
import { yamlHelpContext } from './yaml-help-context'

const escape = (text: string) => text.replace(/[\\`*_{}[\]<>]/g, '\\$&')

/** Adds contextual guidance without disabling the YAML service's diagnostics or
 * schema fallback. A stale request never returns help for an earlier revision. */
export function registerFieldHelp(monaco: typeof Monaco): Monaco.IDisposable {
  return monaco.languages.registerHoverProvider('yaml', {
    async provideHover(model, position, token) {
      const version = model.getVersionId()
      const context = yamlHelpContext(model.uri.path, model.getValue(), model.getOffsetAt(position))
      if (!context) return undefined
      try {
        const catalog = await loadHelpCached(context.kind)
        if (token.isCancellationRequested || model.isDisposed() || version !== model.getVersionId()) return undefined
        const help = resolveFieldHelp(catalog, context.path, context.subtype)
        if (!help) return undefined
        const blocks = [help.summary, ...(help.details ?? [])].map(escape)
        if (help.unsetBehavior) blocks.push(`**When unset:** ${escape(help.unsetBehavior)}`)
        for (const choice of help.choices ?? []) blocks.push(`- **${escape(choice.value)}:** ${escape(choice.explanation)}`)
        for (const constraint of help.constraints ?? []) blocks.push(`- ${escape(constraint)}`)
        for (const example of help.examples ?? []) blocks.push(`${escape(example.label)}\n\n\`\`\`yaml\n${example.yaml.replace(/`/g, '\\`')}\n\`\`\``)
        for (const source of help.sources) {
          const link = helpSourceLink(source)
          blocks.push(`[${escape(link.title)}](${link.href})`)
        }
        const start = model.getPositionAt(context.start), end = model.getPositionAt(context.end)
        return { range: new monaco.Range(start.lineNumber, start.column, end.lineNumber, end.column), contents: [{ value: blocks.join('\n\n'), isTrusted: false }] }
      } catch { return undefined }
    },
  })
}
