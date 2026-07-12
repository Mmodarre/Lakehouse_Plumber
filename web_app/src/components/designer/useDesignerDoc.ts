import { useMemo } from 'react'
import { useQuery } from '@tanstack/react-query'
import { fetchFileContentWithMeta } from '@/api/files'
import { errorMessage } from '@/lib/errors'
import {
  deriveGraph,
  listActions,
  parseFlowgroupFile,
  readFlowgroupMeta,
  readTemplateParams,
  selectFlowgroup,
  selectTemplate,
} from '@/lib/flowgroup-doc'
import type {
  ActionRead,
  FlowgroupGraph,
  FlowgroupMeta,
  TemplateInfo,
  TemplateParamRead,
} from '@/lib/flowgroup-doc'

// ── useDesignerDoc — file fetch + document derivation ────────
//
// One hook instance owns the read path of a designer tab: fetch the
// flowgroup (or template) YAML (same ['file-content', path] key that
// usePushChannel invalidates on SSE file-changed events, so an external edit
// re-renders the canvas), parse it with lib/flowgroup-doc, select the tab's
// document, and derive everything the canvas shows. Read-only: no mutation,
// no save. A template tab (`docKind='template'`) derives its action canvas
// from the template body and additionally exposes the declared parameters.

const NO_ACTIONS: ActionRead[] = []
const NO_WARNINGS: readonly string[] = []
const NO_PARAMS: TemplateParamRead[] = []

export type DesignerDocKind = 'flowgroup' | 'template'

export interface DesignerDoc {
  /** Raw file content (null until the first fetch resolves). */
  content: string | null
  /** First content fetch still in flight. */
  isLoading: boolean
  /** Content fetch failed — user-facing message, `null` otherwise. */
  loadError: string | null
  refetch: () => void
  /** First YAML parse error message; `null` when the file parses. */
  parseError: string | null
  /** File-shape warnings from the parser (duplicate names, mixed syntax, …). */
  fileWarnings: readonly string[]
  /** The tab's flowgroup/template exists in the parsed file. */
  found: boolean
  /** File looks like a blueprint definition/instance (not flowgroup-authorable). */
  blueprintLike: boolean
  meta: FlowgroupMeta | null
  actions: ActionRead[]
  graph: FlowgroupGraph | null
  /** Template authoring mode (the tab's file is a template). */
  isTemplate: boolean
  /** Declared template parameters (empty unless `isTemplate`). */
  templateParams: TemplateParamRead[]
  /** Template metadata (null unless `isTemplate`). */
  templateInfo: TemplateInfo | null
}

interface DerivedDoc {
  parseError: string | null
  fileWarnings: readonly string[]
  found: boolean
  blueprintLike: boolean
  meta: FlowgroupMeta | null
  actions: ActionRead[]
  graph: FlowgroupGraph | null
  isTemplate: boolean
  templateParams: TemplateParamRead[]
  templateInfo: TemplateInfo | null
}

function deriveFlowgroup(content: string, flowgroup: string): DerivedDoc {
  const file = parseFlowgroupFile(content)
  if (file.errors.length > 0) {
    return {
      parseError: file.errors[0].message,
      fileWarnings: file.warnings,
      found: false,
      blueprintLike: false,
      meta: null,
      actions: NO_ACTIONS,
      graph: null,
      isTemplate: false,
      templateParams: NO_PARAMS,
      templateInfo: null,
    }
  }
  const doc = selectFlowgroup(file, flowgroup)
  if (doc === undefined) {
    return {
      parseError: null,
      fileWarnings: file.warnings,
      found: false,
      blueprintLike: file.blueprintLike,
      meta: null,
      actions: NO_ACTIONS,
      graph: null,
      isTemplate: false,
      templateParams: NO_PARAMS,
      templateInfo: null,
    }
  }
  return {
    parseError: null,
    fileWarnings: file.warnings,
    found: true,
    blueprintLike: false,
    meta: readFlowgroupMeta(doc),
    actions: listActions(doc),
    graph: deriveGraph(doc),
    isTemplate: false,
    templateParams: NO_PARAMS,
    templateInfo: null,
  }
}

function deriveTemplate(content: string): DerivedDoc {
  const file = parseFlowgroupFile(content)
  if (file.errors.length > 0) {
    return {
      parseError: file.errors[0].message,
      fileWarnings: file.warnings,
      found: false,
      blueprintLike: false,
      meta: null,
      actions: NO_ACTIONS,
      graph: null,
      isTemplate: true,
      templateParams: NO_PARAMS,
      templateInfo: null,
    }
  }
  const template = selectTemplate(file)
  if (template === undefined) {
    return {
      parseError: null,
      fileWarnings: file.warnings,
      found: false,
      blueprintLike: file.blueprintLike,
      meta: null,
      actions: NO_ACTIONS,
      graph: null,
      isTemplate: true,
      templateParams: NO_PARAMS,
      templateInfo: null,
    }
  }
  return {
    parseError: null,
    fileWarnings: file.warnings,
    found: true,
    blueprintLike: false,
    // Templates carry no flowgroup meta; identity/params come from templateInfo.
    meta: null,
    actions: listActions(template.body),
    graph: deriveGraph(template.body),
    isTemplate: true,
    templateParams: readTemplateParams(template),
    templateInfo: template.info,
  }
}

export function useDesignerDoc(
  filePath: string,
  flowgroup: string,
  docKind: DesignerDocKind = 'flowgroup',
): DesignerDoc {
  const query = useQuery({
    queryKey: ['file-content', filePath],
    queryFn: () => fetchFileContentWithMeta(filePath),
  })

  const content = query.data?.content
  const derived = useMemo(
    () =>
      content === undefined
        ? null
        : docKind === 'template'
          ? deriveTemplate(content)
          : deriveFlowgroup(content, flowgroup),
    [content, flowgroup, docKind],
  )

  return {
    content: content ?? null,
    isLoading: query.isPending,
    loadError: query.isError ? errorMessage(query.error, 'Failed to load file') : null,
    refetch: () => void query.refetch(),
    parseError: derived?.parseError ?? null,
    fileWarnings: derived?.fileWarnings ?? NO_WARNINGS,
    found: derived?.found ?? false,
    blueprintLike: derived?.blueprintLike ?? false,
    meta: derived?.meta ?? null,
    actions: derived?.actions ?? NO_ACTIONS,
    graph: derived?.graph ?? null,
    isTemplate: derived?.isTemplate ?? docKind === 'template',
    templateParams: derived?.templateParams ?? NO_PARAMS,
    templateInfo: derived?.templateInfo ?? null,
  }
}
