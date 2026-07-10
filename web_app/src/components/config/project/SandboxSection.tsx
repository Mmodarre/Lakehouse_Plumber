import { isPlainObject } from '../../../lib/config-model'
import { SectionCard } from '../SectionCard'
import { EnumSelect } from '../fields/EnumSelect'
import { OptionalTextField } from '../fields/OptionalTextField'
import { StringListEditor } from '../fields/StringListEditor'
import { SectionIssues } from './SectionIssues'
import type { ProjectFormApi } from './projectFormSupport'
import { issueText, issuesAtExactly } from './projectFormSupport'

// sandbox section (models/_sandbox.py SandboxConfig, parsed by
// loaders/_sandbox_config_parser.py). Presence skeleton: {} — every field
// has a model default. table_pattern is validated by the config-model
// mirror of the Pydantic validator (both {namespace} and {table} required,
// identifier-fragment literals only).

const BASE = ['sandbox'] as const

export function SandboxSection({ form }: { form: ProjectFormApi }) {
  const raw = form.doc.sandbox
  const present = 'sandbox' in form.doc
  const section = isPlainObject(raw) ? raw : {}
  const broken = present && raw !== null && !isPlainObject(raw)
  const allowedEnvs = Array.isArray(section.allowed_envs) ? section.allowed_envs : undefined

  return (
    <SectionCard
      title="Sandbox"
      description="Team policy for developer sandbox generation (lhp generate --sandbox)."
      presence={{
        present,
        onEnable: () => form.set([...BASE], {}),
        onDisable: () => form.del([...BASE]),
        confirmText: 'Removes the whole sandbox section from lhp.yaml.',
      }}
    >
      <SectionIssues issues={issuesAtExactly(form.issues, [...BASE])} />
      {!broken && (
        <>
          <EnumSelect
            id="sandbox-strategy"
            label="Strategy"
            value={typeof section.strategy === 'string' ? section.strategy : undefined}
            options={['table']}
            unsetLabel="Not set (default: table)"
            onSet={(v) => form.setField([...BASE], 'strategy', v)}
            onUnset={() => form.del([...BASE, 'strategy'])}
            description="How sandbox output is isolated — only table renaming exists today."
            issue={issueText(form.issues, [...BASE, 'strategy'])?.message}
          />
          <OptionalTextField
            id="sandbox-table-pattern"
            label="Table pattern"
            value={section.table_pattern}
            onSet={(v) => form.setField([...BASE], 'table_pattern', v)}
            onUnset={() => form.del([...BASE, 'table_pattern'])}
            monospace
            placeholder="default: {namespace}_{table}"
            description="Must contain {namespace} and {table}; literal text limited to letters, digits, and underscores."
            issue={issueText(form.issues, [...BASE, 'table_pattern'])?.message}
          />
          <StringListEditor
            id="sandbox-allowed-envs"
            label="Allowed environments"
            description="Environments where sandbox mode may run. Remove all to allow every environment."
            value={'allowed_envs' in section && allowedEnvs !== undefined ? allowedEnvs : undefined}
            monospace
            placeholder="e.g. dev"
            onEditItem={(i, v) => form.set([...BASE, 'allowed_envs', i], v)}
            onAddItem={(v) =>
              allowedEnvs === undefined
                ? form.setField([...BASE], 'allowed_envs', [v])
                : form.set([...BASE, 'allowed_envs', allowedEnvs.length], v)
            }
            onRemoveItem={(i) => form.del([...BASE, 'allowed_envs', i])}
            onDeleteKey={() => form.del([...BASE, 'allowed_envs'])}
            issue={issueText(form.issues, [...BASE, 'allowed_envs'])?.message}
            itemIssue={(i) => issueText(form.issues, [...BASE, 'allowed_envs', i])?.message}
          />
        </>
      )}
    </SectionCard>
  )
}
