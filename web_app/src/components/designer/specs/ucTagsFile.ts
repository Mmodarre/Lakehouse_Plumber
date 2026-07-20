// ── uc_tags sidecar — path proposal + skeleton for the tags_file field ─
//
// Single-sourced for both write specs (streaming_table + materialized_view)
// so the `tags_file` "New" affordance behaves identically. It proposes
// `uc_tags/<table>.yaml` and seeds a commented skeleton keyed to the write
// target's `table`.
//
// Two deliberate behaviors the generator forces on us:
//  • A token-bearing (`${…}`/`{{…}}`/`%{…}`) or absent `table` yields NO path
//    proposal AND NO `table:` prefill — the generator compares the sidecar's
//    declared `table`/`name` against the SUBSTITUTED write target, so seeding a
//    raw token guarantees a CFG-068 mismatch warning (a plain comment is safe
//    instead).
//  • The skeleton NEVER seeds `tags: {}` — a managed-empty map deletes every
//    table tag under `remove_undeclared_tags`. Every block starts commented,
//    so the file does not parse until the user opts one in — intentional
//    guidance, not a bug.

import type { YamlPath } from '@/lib/flowgroup-doc'
import { isSubstitutionToken, readPath } from './helpers'

const TABLE_PATH: YamlPath = ['write_target', 'table']

/**
 * The write target's `table` as a clean, tokenless string, or `null`. Mirrors
 * `companionCheckablePath`'s token detection (via `isSubstitutionToken`).
 */
export function ucTagsTableName(raw: Record<string, unknown>): string | null {
  const value = readPath(raw, TABLE_PATH)
  if (typeof value !== 'string') return null
  const table = value.trim()
  if (table === '') return null
  if (isSubstitutionToken(table)) return null
  return table
}

/** Proposed sidecar path `uc_tags/<table>.yaml`, or `null` when no clean table. */
export function ucTagsSuggestPath(raw: Record<string, unknown>): string | null {
  const table = ucTagsTableName(raw)
  return table === null ? null : `uc_tags/${table}.yaml`
}

/**
 * The commented skeleton the New/Create affordance seeds. `table:` is prefilled
 * when the write target's `table` is a clean string; otherwise a comment stands
 * in (never a raw token — see the module note).
 */
export function ucTagsStub(raw: Record<string, unknown>): string {
  const table = ucTagsTableName(raw)
  const tableLine =
    table === null ? "table:  # set to the write target's table name" : `table: ${table}`
  return `version: 1.0.0
${tableLine}

# Uncomment at least one block — a tags file must declare
# 'tags:' (table-level) and/or 'columns:' (column-level).

# tags:
#   team: data-eng
#   cost_center: "1234"

# columns:
#   - name: email
#     tags:
#       pii: high
#       masked: ''
`
}
