import { useMemo } from 'react'

import { documentCount, toJS, type ConfigFileHandle } from '../../../lib/yaml-doc'
import {
  validateJobConfigFile,
  validatePipelineConfigFile,
  validateProjectConfig,
  type ValidationIssue as ConfigModelIssue,
} from '../../../lib/config-model'
import { useDocumentStore } from '../../../store/documentStore'
import type { ConfigKind, WorkspaceTabRef } from '../../../store/workspaceStore'
import type { ValidationIssue } from '../../../types/api'
import type { ScopedIssues } from './scopeIssues'

// ── configIssues — client-side config validator issues for the Inspector ──
//
// The three config surfaces validate client-side (lib/config-model/*) — the
// same pure validators the config Form view renders per-field. Those issues
// never enter runStore (which holds run-validation output). This layer re-runs
// the matching validator over the active config tab's parsed document (owned by
// documentStore, keyed by `version`) and maps the findings onto the api
// `ValidationIssue` shape the Inspector's IssueList renders — a source-merge
// scoped to the active config tab, with no runStore pollution (§6.3 / §3).
//
// Availability: the document handle is present whenever the config tab's Form
// view is mounted (ConfigFormView opens + reparses it). In the raw-YAML view no
// handle is open, so this yields nothing and the Inspector falls back to
// run-validation issues for that file — a documented v1 limitation.

const NONE: ValidationIssue[] = []

/** Map a config-model issue (doc-index + YAML-path addressed) onto the api
 * `ValidationIssue` the Inspector renders. `file_path` is the config file so a
 * row click opens it; there is no line number (config issues address a path,
 * not a source position). */
function toApiIssue(issue: ConfigModelIssue, filePath: string): ValidationIssue {
  const where = issue.path.length > 0 ? issue.path.join('.') : '(document)'
  return {
    code: issue.code ?? 'CONFIG',
    category: 'config',
    severity: issue.severity,
    title: issue.message,
    details: `at ${where}`,
    pipeline_name: null,
    flowgroup_name: null,
    file_path: filePath,
    suggestions: [],
    context: {},
    doc_link: null,
  }
}

function runValidator(configKind: ConfigKind, docs: unknown[]): ConfigModelIssue[] {
  switch (configKind) {
    case 'project':
      return validateProjectConfig(docs[0])
    case 'pipeline':
      return validatePipelineConfigFile(docs)
    case 'job':
      return validateJobConfigFile(docs)
  }
}

/** Client-side validator issues for the active config tab, else `[]`. Recomputes
 * when the document object changes (documentStore replaces it on every
 * mutate/reparse — the `version` bump). */
export function useActiveConfigIssues(activeTab: WorkspaceTabRef | null): ValidationIssue[] {
  const configTab = activeTab?.kind === 'config' ? activeTab : null
  const configPath = configTab?.path ?? null
  const configKind = configTab?.configKind ?? null
  const doc = useDocumentStore((s) => (configPath !== null ? (s.docs[configPath] ?? null) : null))

  return useMemo(() => {
    if (
      configPath === null ||
      configKind === null ||
      doc === null ||
      doc.handle === null ||
      doc.errors.length > 0
    ) {
      return NONE
    }
    const handle = doc.handle as ConfigFileHandle
    const count = documentCount(handle)
    const docs: unknown[] = []
    for (let i = 0; i < count; i++) docs.push(toJS(handle, i))
    const raw = runValidator(configKind, docs)
    if (raw.length === 0) return NONE
    return raw.map((issue) => toApiIssue(issue, configPath))
  }, [configPath, configKind, doc])
}

/** Strip a leading slash so reported `file_path`s compare against the
 * project-relative paths tabs carry. */
function toProjectRelative(path: string): string {
  return path.replace(/^\/+/, '')
}

/**
 * Merge the active config tab's client-side validator issues into the
 * run-scoped set. For a config tab with client-side issues, show them plus any
 * run-validation issues for THIS file (never the project-wide fallback
 * `scopeByFile` may return). Otherwise the run-scoped set is returned unchanged.
 */
export function mergeConfigIssues(
  runScoped: ScopedIssues,
  configIssues: ValidationIssue[],
  activeTab: WorkspaceTabRef | null,
  allIssues: readonly ValidationIssue[],
): ScopedIssues {
  if (configIssues.length === 0 || activeTab?.kind !== 'config') return runScoped
  const path = activeTab.path
  const fileRun = allIssues.filter(
    (i) => i.file_path !== null && toProjectRelative(i.file_path) === path,
  )
  return {
    issues: [...configIssues, ...fileRun],
    scope: { label: path.split('/').pop() ?? path, projectWide: false },
  }
}
