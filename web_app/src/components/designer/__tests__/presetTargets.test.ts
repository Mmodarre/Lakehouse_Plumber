import { describe, expect, it } from 'vitest'
import { presetTargetsSubType } from '../presetTargets'

// Fixtures match the REAL GET /api/presets/{name} shape:
//   • `resolved` is the inheritance-merged defaults payload DIRECTLY
//     (e.g. { write_actions: { streaming_table: {...} } }) — it has NO
//     top-level `defaults` key (routers/presets.py: resolved=dict(merged_config);
//     merged_config = preset.defaults).
//   • `raw` is the full preset file, so its `defaults:` wrapper IS present.

/** resolved = the merged defaults payload directly (the common case). */
const viaResolved = (defaults: Record<string, unknown>) => ({ resolved: defaults, raw: {} })
/** resolved absent → fall back to raw, which HAS the `defaults:` wrapper. */
const viaRaw = (defaults: Record<string, unknown>) => ({ resolved: null, raw: { defaults } })

describe('presetTargetsSubType', () => {
  it('matches a non-empty block in resolved (the endpoint shape)', () => {
    const p = viaResolved({
      write_actions: { streaming_table: { table_properties: { a: 'b' } } },
    })
    expect(presetTargetsSubType(p, 'write', 'streaming_table')).toBe(true)
  })

  it('ignores an empty block', () => {
    const p = viaResolved({ load_actions: { cloudfiles: {} } })
    expect(presetTargetsSubType(p, 'load', 'cloudfiles')).toBe(false)
  })

  it('does not match a different sub-type or kind', () => {
    const p = viaResolved({ write_actions: { streaming_table: { comment: 'x' } } })
    expect(presetTargetsSubType(p, 'write', 'materialized_view')).toBe(false)
    expect(presetTargetsSubType(p, 'load', 'streaming_table')).toBe(false)
  })

  it('falls back to raw.defaults when resolved is absent', () => {
    const p = viaRaw({ transform_actions: { sql: { spark_conf: { x: '1' } } } })
    expect(presetTargetsSubType(p, 'transform', 'sql')).toBe(true)
  })

  it('prefers resolved over raw when both are present', () => {
    // resolved carries the effective (merged) defaults; raw alone would be empty.
    const p = {
      resolved: { write_actions: { streaming_table: { table_properties: { a: 'b' } } } },
      raw: { defaults: { write_actions: { streaming_table: {} } } },
    }
    expect(presetTargetsSubType(p, 'write', 'streaming_table')).toBe(true)
  })

  it('tolerates missing / malformed defaults', () => {
    expect(presetTargetsSubType({ resolved: null, raw: {} }, 'load', 'cloudfiles')).toBe(false)
    expect(presetTargetsSubType(viaResolved({}), 'load', 'cloudfiles')).toBe(false)
    expect(
      presetTargetsSubType({ resolved: null, raw: { defaults: 'nope' } }, 'load', 'cloudfiles'),
    ).toBe(false)
  })
})
