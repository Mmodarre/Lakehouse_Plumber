import { useQueries } from '@tanstack/react-query'
import { fetchPresetDetail } from '@/api/presets'
import { isPlainObject } from '@/lib/config-model'
import type { ActionKind } from '@/lib/flowgroup-doc'
import type { PresetDetailResponse } from '@/types/api'

// ── Preset badge detection ───────────────────────────────────
//
// A preset's `defaults` are keyed `{kind}_actions.{subType}` (e.g.
// `defaults.write_actions.streaming_table`). An action is "affected by" a
// preset when the flowgroup lists that preset AND the preset defines a
// non-empty defaults block for the action's (kind, subType). The resolved
// (inheritance-merged) view is preferred over the raw file.

function pickDefaults(
  preset: Pick<PresetDetailResponse, 'resolved' | 'raw'>,
): Record<string, unknown> | undefined {
  // The endpoint's `resolved` is ALREADY the inheritance-merged defaults
  // PAYLOAD — it has NO top-level `defaults` key (routers/presets.py:
  // `resolved=dict(merged_config)`; `merged_config = preset.defaults`;
  // schemas/preset.py: "resolved carries the inheritance-merged defaults
  // payload"). Only `raw` is the full preset file whose `defaults:` wrapper
  // must be unwrapped.
  if (isPlainObject(preset.resolved)) return preset.resolved
  const rawDefaults = isPlainObject(preset.raw) ? preset.raw.defaults : undefined
  return isPlainObject(rawDefaults) ? rawDefaults : undefined
}

/** True when a preset's defaults carry a non-empty block for (kind, subType). */
export function presetTargetsSubType(
  preset: Pick<PresetDetailResponse, 'resolved' | 'raw'>,
  kind: ActionKind,
  subType: string,
): boolean {
  const defaults = pickDefaults(preset)
  const bucket = defaults?.[`${kind}_actions`]
  if (!isPlainObject(bucket)) return false
  const forSubType = bucket[subType]
  return isPlainObject(forSubType) && Object.keys(forSubType).length > 0
}

/**
 * Names of `presets` whose defaults target this action's (kind, subType).
 * Fetches each preset detail (shared `['preset', name]` cache with
 * usePresetDetail); returns `[]` while details load or when kind/subType is
 * unknown.
 */
export function useActionPresetBadges(
  presets: readonly string[],
  kind: ActionKind | undefined,
  subType: string | undefined,
): string[] {
  const queries = useQueries({
    queries: presets.map((name) => ({
      queryKey: ['preset', name],
      queryFn: () => fetchPresetDetail(name),
    })),
  })
  if (kind === undefined || subType === undefined) return []
  const targeted: string[] = []
  queries.forEach((query, index) => {
    if (query.data && presetTargetsSubType(query.data, kind, subType)) {
      targeted.push(presets[index])
    }
  })
  return targeted
}
