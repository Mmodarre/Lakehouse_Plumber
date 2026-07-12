// ── newFlowgroupDoc — creation-time YAML assembly (pure) ─────
//
// Turns the Create-flowgroup dialog's inputs into the YAML text written
// through the files API, plus the conventional file path and the name
// validation the dialog enforces. No React, no network — unit-testable.
//
// Ground truth (verified against src/lhp):
//  • Minimal flowgroup — models/_flowgroup.py:12-20: `pipeline` + `flowgroup`
//    are the only required fields; `actions` defaults to []. Blank writes all
//    three so the file is unmistakably a flowgroup and opens straight into
//    compose mode.
//  • Template instance — same model: `use_template` + `template_parameters`.
//  • Blueprint instance — models/_blueprint.py:57-137 (new syntax, the legacy
//    `blueprint:` form is deprecated): `use_blueprint` + a nested
//    `parameters:` block. An instance has NO pipeline / flowgroup of its own —
//    those live inside the blueprint's flowgroup specs.
//  • File path — discovery keys on the in-file `pipeline` field, not the
//    folder (e.g. pipelines/01_raw/domain_t/x.yaml declares `pipeline:
//    domain_t_raw`), so any path under pipelines/ is valid; the dialog uses
//    the predictable `pipelines/<pipeline>/<subdir?>/<name>.yaml` convention.

import { stringify } from 'yaml'

/** Filesystem-safe identifier: letters, digits, hyphen, underscore. */
export const NAME_PATTERN = /^[a-zA-Z0-9_-]+$/

const YAML_OPTS = { lineWidth: 0 } as const

/** Non-null validation message for a flowgroup/pipeline/instance name, else null. */
export function validateName(name: string): string | null {
  if (name === '') return null
  if (!NAME_PATTERN.test(name)) return 'Only letters, numbers, hyphens, and underscores'
  return null
}

/** The conventional path for a flowgroup in `pipeline`, under an optional
 * `subdir`. Both are assumed already validated. */
export function flowgroupFilePath(pipeline: string, subdir: string, name: string): string {
  const parts = ['pipelines', pipeline]
  if (subdir) parts.push(subdir)
  parts.push(`${name}.yaml`)
  return parts.join('/')
}

/** The path for a blueprint instance file under pipelines/. */
export function blueprintInstancePath(instanceName: string): string {
  return `pipelines/${instanceName}.yaml`
}

/** Minimal blank flowgroup: `{ pipeline, flowgroup, actions: [] }`. */
export function buildBlankFlowgroupYaml(pipeline: string, flowgroup: string): string {
  return stringify({ pipeline, flowgroup, actions: [] }, YAML_OPTS)
}

/** Template-based flowgroup: `use_template` + optional `template_parameters`. */
export function buildTemplateFlowgroupYaml(
  pipeline: string,
  flowgroup: string,
  template: string,
  params: Record<string, unknown>,
): string {
  const doc: Record<string, unknown> = { pipeline, flowgroup, use_template: template }
  if (Object.keys(params).length > 0) doc.template_parameters = params
  return stringify(doc, YAML_OPTS)
}

/** Blueprint instance (new syntax): `use_blueprint` + optional `parameters`. */
export function buildBlueprintInstanceYaml(
  blueprint: string,
  params: Record<string, unknown>,
): string {
  const doc: Record<string, unknown> = { use_blueprint: blueprint }
  if (Object.keys(params).length > 0) doc.parameters = params
  return stringify(doc, YAML_OPTS)
}
