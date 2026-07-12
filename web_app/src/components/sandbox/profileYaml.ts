// ── Sandbox profile assembly — pure, testable ───────────────
//
// Builds the `.lhp/profile.yaml` body the picker PUTs through the files API.
// The file is the personal, gitignored sandbox profile validated by
// `SandboxProfile` (src/lhp/models/_sandbox.py): a top-level `sandbox:` key
// wrapping `namespace` (a scalar) and `pipelines` (a non-empty list of names
// or globs) — exactly the loader's own example
// (src/lhp/core/loaders/sandbox_profile_loader.py).

import { stringify } from 'yaml'
import {
  parseConfigFile,
  serializeConfigFile,
  setPath,
} from '../../lib/yaml-doc'

/** The `sandbox:` payload — mirrors `SandboxProfile`. */
export interface SandboxProfileData {
  namespace: string
  pipelines: string[]
}

/**
 * Assemble the `.lhp/profile.yaml` text for `{namespace, pipelines}`.
 *
 * With no existing source (or an empty / unparseable one) a fresh document is
 * emitted plainly — `yaml.stringify` quotes any pattern that would otherwise
 * re-parse as an alias (`*_bronze`) or another type. When an existing source
 * is given and parses cleanly, the whole `sandbox:` node is replaced through
 * the comment-preserving layer so a file header and any sibling keys survive
 * the write (replacing the node in one AST edit avoids a stale-scalar hazard).
 */
export function assembleProfileYaml(
  { namespace, pipelines }: SandboxProfileData,
  existingSource?: string | null,
): string {
  const payload = { sandbox: { namespace, pipelines } }

  if (existingSource && existingSource.trim() !== '') {
    const handle = parseConfigFile(existingSource)
    if (handle.errors.length === 0) {
      setPath(handle, 0, ['sandbox'], { namespace, pipelines })
      return serializeConfigFile(handle)
    }
  }

  return stringify(payload)
}

/**
 * Clean a list of pipeline names / glob patterns for the profile: trim each
 * entry, drop blanks, and dedupe while preserving first-seen order.
 */
export function normalizePipelines(entries: readonly string[]): string[] {
  const seen = new Set<string>()
  const out: string[] = []
  for (const raw of entries) {
    const entry = raw.trim()
    if (entry === '' || seen.has(entry)) continue
    seen.add(entry)
    out.push(entry)
  }
  return out
}
