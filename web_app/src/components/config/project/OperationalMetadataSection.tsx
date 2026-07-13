import { isPlainObject } from '../../../lib/config-model'
import { SectionCard } from '../SectionCard'
import { KeyValueMapEditor } from '../fields/KeyValueMapEditor'
import { OpMetadataColumns } from './OpMetadataColumns'
import { OpMetadataPresets } from './OpMetadataPresets'
import { SectionIssues } from './SectionIssues'
import type { ProjectFormApi } from './projectFormSupport'
import { issueText, issuesAtExactly } from './projectFormSupport'

// operational_metadata section (models/_operational_metadata.py, parsed by
// loaders/_operational_metadata_config_parser.py). Presence skeleton: {}
// (the section has no `enabled` — an empty map is the smallest valid
// form). Columns/presets live in their own editors (shorthand handling);
// defaults is a free str→str map with the non-string protection rules.

const BASE = ['operational_metadata'] as const

export function OperationalMetadataSection({ form }: { form: ProjectFormApi }) {
  const raw = form.doc.operational_metadata
  const present = 'operational_metadata' in form.doc
  const section = isPlainObject(raw) ? raw : {}
  const broken = present && !isPlainObject(raw)

  const columns = isPlainObject(section.columns) ? section.columns : undefined
  const presets = isPlainObject(section.presets) ? section.presets : undefined
  const defaults = isPlainObject(section.defaults) ? section.defaults : undefined
  const defaultsPath = [...BASE, 'defaults']
  const defaultsIssue = issueText(form.issues, defaultsPath)

  return (
    <SectionCard
      title="Operational metadata"
      description="Metadata columns automatically added to generated tables."
      presence={{
        present,
        onEnable: () => form.set([...BASE], {}),
        onDisable: () => form.del([...BASE]),
        confirmText: 'Removes the whole operational_metadata section from lhp.yaml.',
      }}
    >
      <SectionIssues issues={issuesAtExactly(form.issues, [...BASE])} />
      {!broken && (
        <>
          <OpMetadataColumns form={form} columns={columns} />
          <OpMetadataPresets form={form} presets={presets} />
          <KeyValueMapEditor
            id="om-defaults"
            label="Defaults"
            helpPath={['operational_metadata', 'defaults']}
            value={'defaults' in section ? defaults : undefined}
            onSetEntry={(k, v) =>
              defaults === undefined
                ? form.setField([...BASE], 'defaults', { [k]: v })
                : form.set([...defaultsPath, k], v)
            }
            onRenameEntry={(oldKey, newKey) => {
              // Rename preserves the RAW value (never coerces): delete+set.
              const value = defaults?.[oldKey]
              form.del([...defaultsPath, oldKey])
              form.set([...defaultsPath, newKey], value)
            }}
            onRemoveEntry={(k) => form.del([...defaultsPath, k])}
            onDeleteKey={() => form.del(defaultsPath)}
            issue={defaultsIssue?.message}
            issueSeverity={defaultsIssue?.severity}
          />
        </>
      )}
    </SectionCard>
  )
}
