import { isPlainObject, parseLaxBool } from '../../../lib/config-model'
import { SectionCard } from '../SectionCard'
import { BoolSwitch } from '../fields/BoolSwitch'
import { OptionalTextField } from '../fields/OptionalTextField'
import { SectionIssues } from './SectionIssues'
import type { ProjectFormApi } from './projectFormSupport'
import { issueText, issuesAtExactly } from './projectFormSupport'

// event_log section (models/_monitoring.py EventLogConfig, parsed by
// loaders/_event_log_config_parser.py). Presence skeleton: {enabled: true}
// — when enabled, the loader REQUIRES catalog + schema, so the validator
// immediately points the user at the two fields to fill in.

const BASE = ['event_log'] as const

export function EventLogSection({ form }: { form: ProjectFormApi }) {
  const raw = form.doc.event_log
  const present = 'event_log' in form.doc
  const section = isPlainObject(raw) ? raw : {}
  // A present non-mapping (e.g. `event_log: true`) is loader-fatal: show
  // the issue, hide the fields (writes into a scalar cannot be surgical).
  const broken = present && raw !== null && !isPlainObject(raw)

  return (
    <SectionCard
      title="Event log"
      description="Where generated pipelines write their Lakeflow event logs."
      presence={{
        present,
        onEnable: () => form.set([...BASE], { enabled: true }),
        onDisable: () => form.del([...BASE]),
        confirmText: 'Removes the whole event_log section from lhp.yaml.',
      }}
    >
      <SectionIssues issues={issuesAtExactly(form.issues, [...BASE])} />
      {!broken && (
        <>
          <BoolSwitch
            id="event-log-enabled"
            label="Enabled"
            value={'enabled' in section ? parseLaxBool(section.enabled) : undefined}
            defaultValue={true}
            onSet={(v) => form.setField([...BASE], 'enabled', v)}
            onReset={() => form.del([...BASE, 'enabled'])}
            issue={issueText(form.issues, [...BASE, 'enabled'])?.message}
          />
          <OptionalTextField
            id="event-log-catalog"
            label="Catalog"
            value={section.catalog}
            onSet={(v) => form.setField([...BASE], 'catalog', v)}
            onUnset={() => form.del([...BASE, 'catalog'])}
            monospace
            helpPath={['event_log', 'catalog']}
            issue={issueText(form.issues, [...BASE, 'catalog'])?.message}
          />
          <OptionalTextField
            id="event-log-schema"
            label="Schema"
            value={section.schema}
            onSet={(v) => form.setField([...BASE], 'schema', v)}
            onUnset={() => form.del([...BASE, 'schema'])}
            monospace
            helpPath={['event_log', 'schema']}
            issue={issueText(form.issues, [...BASE, 'schema'])?.message}
          />
          <OptionalTextField
            id="event-log-name-prefix"
            label="Name prefix"
            value={section.name_prefix}
            onSet={(v) => form.setField([...BASE], 'name_prefix', v)}
            onUnset={() => form.del([...BASE, 'name_prefix'])}
            monospace
            issue={issueText(form.issues, [...BASE, 'name_prefix'])?.message}
          />
          <OptionalTextField
            id="event-log-name-suffix"
            label="Name suffix"
            value={section.name_suffix}
            onSet={(v) => form.setField([...BASE], 'name_suffix', v)}
            onUnset={() => form.del([...BASE, 'name_suffix'])}
            monospace
            issue={issueText(form.issues, [...BASE, 'name_suffix'])?.message}
          />
        </>
      )}
    </SectionCard>
  )
}
