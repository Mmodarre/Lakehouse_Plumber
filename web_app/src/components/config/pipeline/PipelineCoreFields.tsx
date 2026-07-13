import {
  PIPELINE_ALLOWED_CHANNELS,
  PIPELINE_ALLOWED_EDITIONS,
  PIPELINE_ALLOWED_PACKAGING_MODES,
  PIPELINE_BUILTIN_DEFAULTS,
} from '../../../lib/config-model'
import { SectionCard } from '../SectionCard'
import { BoolSwitch } from '../fields/BoolSwitch'
import { EnumSelect } from '../fields/EnumSelect'
import { OptionalTextField } from '../fields/OptionalTextField'
import type { DocFormApi } from '../shared/docFormSupport'

// ── PipelineCoreFields — compute / runtime / target scalars ──
//
// The scalar settings the bundle template renders explicitly
// (pipeline_resource.yml.j2) plus `packaging` (LHP-internal, consumed by
// resolve_packaging_modes and stripped before render). Every field keeps
// unset-vs-explicit semantics: deleting the key restores inheritance from
// project defaults / built-ins.

/** Present-and-boolean, else undefined (BoolSwitch shows the default). */
function boolAt(settings: Record<string, unknown>, key: string): boolean | undefined {
  const value = settings[key]
  return typeof value === 'boolean' ? value : undefined
}

/** Present values display even when invalid (validator supplies the issue). */
function stringAt(settings: Record<string, unknown>, key: string): string | undefined {
  return key in settings ? String(settings[key]) : undefined
}

export function PipelineCoreFields({ api, idPrefix }: { api: DocFormApi; idPrefix: string }) {
  return (
    <>
      <SectionCard
        title="Compute & runtime"
        description="Unset fields inherit from project defaults, then LHP's built-ins."
      >
        <BoolSwitch
          id={`${idPrefix}-serverless`}
          label="Serverless"
          value={boolAt(api.settings, 'serverless')}
          defaultValue={PIPELINE_BUILTIN_DEFAULTS.serverless}
          onSet={(value) => api.set(['serverless'], value)}
          onReset={() => api.del(['serverless'])}
          helpPath={['serverless']}
          issue={api.issueAt(['serverless'])?.message}
        />
        <EnumSelect
          id={`${idPrefix}-edition`}
          label="Edition"
          value={stringAt(api.settings, 'edition')}
          options={PIPELINE_ALLOWED_EDITIONS}
          unsetLabel={`Not set (default: ${PIPELINE_BUILTIN_DEFAULTS.edition})`}
          onSet={(value) => api.set(['edition'], value)}
          onUnset={() => api.del(['edition'])}
          helpPath={['edition']}
          issue={api.issueAt(['edition'])?.message}
        />
        <EnumSelect
          id={`${idPrefix}-channel`}
          label="Channel"
          value={stringAt(api.settings, 'channel')}
          options={PIPELINE_ALLOWED_CHANNELS}
          unsetLabel={`Not set (default: ${PIPELINE_BUILTIN_DEFAULTS.channel})`}
          onSet={(value) => api.set(['channel'], value)}
          onUnset={() => api.del(['channel'])}
          issue={api.issueAt(['channel'])?.message}
        />
        <BoolSwitch
          id={`${idPrefix}-continuous`}
          label="Continuous"
          value={boolAt(api.settings, 'continuous')}
          defaultValue={PIPELINE_BUILTIN_DEFAULTS.continuous}
          onSet={(value) => api.set(['continuous'], value)}
          onReset={() => api.del(['continuous'])}
          helpPath={['continuous']}
          issue={api.issueAt(['continuous'])?.message}
        />
        <BoolSwitch
          id={`${idPrefix}-photon`}
          label="Photon"
          value={boolAt(api.settings, 'photon')}
          defaultValue={false}
          onSet={(value) => api.set(['photon'], value)}
          onReset={() => api.del(['photon'])}
          helpPath={['photon']}
          issue={api.issueAt(['photon'])?.message}
        />
        <EnumSelect
          id={`${idPrefix}-packaging`}
          label="Packaging"
          value={stringAt(api.settings, 'packaging')}
          options={PIPELINE_ALLOWED_PACKAGING_MODES}
          unsetLabel="Not set (default: source)"
          onSet={(value) => api.set(['packaging'], value)}
          onUnset={() => api.del(['packaging'])}
          helpPath={['packaging']}
          issue={api.issueAt(['packaging'])?.message}
        />
      </SectionCard>

      <SectionCard
        title="Target"
        description="Both catalog and schema must resolve for bundle generation (here or in project defaults)."
      >
        <OptionalTextField
          id={`${idPrefix}-catalog`}
          label="Catalog"
          value={api.settings.catalog}
          onSet={(value) => api.set(['catalog'], value)}
          onUnset={() => api.del(['catalog'])}
          placeholder="${catalog} or a literal name"
          monospace
          issue={api.issueAt(['catalog'])?.message}
        />
        <OptionalTextField
          id={`${idPrefix}-schema`}
          label="Schema"
          value={api.settings.schema}
          onSet={(value) => api.set(['schema'], value)}
          onUnset={() => api.del(['schema'])}
          placeholder="${schema} or a literal name"
          monospace
          issue={api.issueAt(['schema'])?.message}
        />
      </SectionCard>
    </>
  )
}
