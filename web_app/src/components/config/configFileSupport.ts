import type { FileNode } from '../../types/api'

// ── Pure support logic for the Config section ────────────────
//
// File filtering / sorting for ConfigFilePicker and path validation for
// CreateFromTemplateDialog, extracted as plain functions so they are
// testable without component plumbing.

/** The two file-backed Config tabs (the project tab is fixed to lhp.yaml). */
export type ConfigTabKind = 'pipeline' | 'job'

/**
 * Single source for each file tab's heading + one-liner — rendered by the
 * tab's editor shell AND by the page's no-file empty state (Task-8 review:
 * previously duplicated between the two).
 */
export const CONFIG_TAB_COPY: Record<ConfigTabKind, { title: string; description: string }> = {
  pipeline: {
    title: 'Pipeline configuration',
    description: 'pipeline_config files — defaults and per-pipeline overrides.',
  },
  job: {
    title: 'Job configuration',
    description: 'job_config and monitoring_job_config files — orchestration job settings.',
  },
}

/** Template kinds served by GET /api/config-templates/{kind}. */
export type ConfigTemplateKind = 'pipeline_config' | 'job_config' | 'monitoring_job_config'

/** Template kinds offered by the create dialog on each tab. */
export const TEMPLATE_KINDS_BY_TAB: Record<ConfigTabKind, readonly ConfigTemplateKind[]> = {
  pipeline: ['pipeline_config'],
  job: ['job_config', 'monitoring_job_config'],
}

/** Human labels for the template kinds. */
export const TEMPLATE_KIND_LABELS: Record<ConfigTemplateKind, string> = {
  pipeline_config: 'Pipeline config',
  job_config: 'Job config',
  monitoring_job_config: 'Monitoring job config',
}

function isYamlName(name: string): boolean {
  return name.endsWith('.yaml') || name.endsWith('.yml')
}

/** Flatten the recursive files tree into every file path (depth-first). */
export function listAllFilePaths(root: FileNode | undefined): string[] {
  if (!root) return []
  const paths: string[] = []
  const walk = (node: FileNode): void => {
    if (node.type === 'file') {
      paths.push(node.path)
      return
    }
    for (const child of node.children ?? []) walk(child)
  }
  walk(root)
  return paths
}

/**
 * Does the basename mark this file as the tab's preferred config kind
 * (`pipeline_config*` for the pipeline tab, `job_config*` /
 * `monitoring_job_config*` for the job tab)?
 */
export function isPreferredForKind(path: string, kind: ConfigTabKind): boolean {
  const name = path.split('/').pop() ?? path
  if (kind === 'pipeline') return name.startsWith('pipeline_config')
  return name.startsWith('job_config') || name.startsWith('monitoring_job_config')
}

/**
 * All `config/**` YAML files from the tree, the tab's preferred kind
 * first, alphabetical within each group. Any config/ YAML remains
 * choosable — preference only affects ordering.
 */
export function listConfigYamlFiles(root: FileNode | undefined, kind: ConfigTabKind): string[] {
  return listAllFilePaths(root)
    .filter((p) => p.startsWith('config/') && isYamlName(p))
    .sort((a, b) => {
      const prefA = isPreferredForKind(a, kind)
      const prefB = isPreferredForKind(b, kind)
      if (prefA !== prefB) return prefA ? -1 : 1
      return a.localeCompare(b)
    })
}

/** Default new-file path for a template kind + environment suffix. */
export function defaultTemplatePath(kind: ConfigTemplateKind, env: string): string {
  return `config/${kind}_${env}.yaml`
}

const SEGMENT_PATTERN = /^[A-Za-z0-9._-]+$/

/**
 * Validate a new config file path (project-relative). Returns a
 * user-facing error message, or `null` when the path is acceptable.
 * `existingPaths` is the flat set of paths already in the files tree —
 * creation must not overwrite (the PUT additionally enforces this
 * atomically via a never-matching If-Match).
 */
export function validateNewConfigPath(
  path: string,
  existingPaths: ReadonlySet<string>,
): string | null {
  const trimmed = path.trim()
  if (trimmed === '') return 'Enter a file name'
  if (!trimmed.startsWith('config/')) return 'The file must live under config/'
  if (!isYamlName(trimmed)) return 'The file name must end with .yaml'
  const segments = trimmed.split('/')
  const basename = segments[segments.length - 1]
  if (basename.replace(/\.(yaml|yml)$/, '') === '') return 'Enter a file name'
  for (const segment of segments) {
    if (segment === '' || segment === '.' || segment === '..') return 'Invalid path'
    if (!SEGMENT_PATTERN.test(segment)) {
      return 'Only letters, digits, ".", "_" and "-" are allowed'
    }
  }
  if (existingPaths.has(trimmed)) return 'This file already exists'
  return null
}
