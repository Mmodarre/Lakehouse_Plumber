import { isPlainObject } from '../../../lib/config-model'
import { SectionCard } from '../SectionCard'
import { KeyValueMapEditor } from '../fields/KeyValueMapEditor'
import type { DocFormApi } from '../shared/docFormSupport'

// ── PipelineMapsFields — configuration + tags map editors ────
//
// `configuration` is a str→str map the loader HARD-fails on non-string
// values for (VAL_009), so non-string rows show the editor's "not text"
// badge AND count as blocking errors. `tags` values are rendered verbatim
// by the bundle template — non-strings are legal there, the editor just
// requires the unlock step before coercing them to text.

function kvHandlers(api: DocFormApi, key: string) {
  const raw = api.settings[key]
  const map = isPlainObject(raw) ? raw : undefined
  return {
    value: map,
    onSetEntry: (entryKey: string, value: string) => api.set([key, entryKey], value),
    // Rename preserves the RAW value (a rename alone never coerces).
    onRenameEntry: (oldKey: string, newKey: string) => {
      const rawValue = map?.[oldKey]
      api.del([key, oldKey])
      api.set([key, newKey], rawValue)
    },
    onRemoveEntry: (entryKey: string) => api.del([key, entryKey]),
    onDeleteKey: () => api.del([key]),
  }
}

export function PipelineMapsFields({ api, idPrefix }: { api: DocFormApi; idPrefix: string }) {
  return (
    <>
      <SectionCard title="Pipeline configuration">
        <KeyValueMapEditor
          id={`${idPrefix}-configuration`}
          label="Spark / SDP configuration"
          {...kvHandlers(api, 'configuration')}
          description="All values must be strings — the loader rejects anything else."
          issue={api.issueAt(['configuration'])?.message}
        />
      </SectionCard>

      <SectionCard title="Tags">
        <KeyValueMapEditor
          id={`${idPrefix}-tags`}
          label="Tags"
          {...kvHandlers(api, 'tags')}
          description="Classic compute only (serverless tags come from the compute policy). Values render as written."
          issue={api.issueAt(['tags'])?.message}
        />
      </SectionCard>
    </>
  )
}
