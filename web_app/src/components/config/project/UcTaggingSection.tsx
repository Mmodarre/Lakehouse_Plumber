import { isPlainObject } from '../../../lib/config-model'
import { SectionCard } from '../SectionCard'
import { BoolSwitch } from '../fields/BoolSwitch'
import { OptionalNumberField } from '../fields/OptionalNumberField'
import { SectionIssues } from './SectionIssues'
import type { ProjectFormApi } from './projectFormSupport'
import { issueText, issuesAtExactly } from './projectFormSupport'

// uc_tagging section (loaders/_uc_tagging_config_parser.py). Presence
// skeleton: {enabled: true}. NOTE the parser checks isinstance(x, bool) —
// STRICT booleans — so only real booleans flow into the switches here
// ("yes"/1 render as unset and the validator flags them).

const BASE = ['uc_tagging'] as const

export function UcTaggingSection({ form }: { form: ProjectFormApi }) {
  const raw = form.doc.uc_tagging
  const present = 'uc_tagging' in form.doc
  const section = isPlainObject(raw) ? raw : {}
  const broken = present && raw !== null && !isPlainObject(raw)
  const strictBool = (key: string): boolean | undefined =>
    typeof section[key] === 'boolean' ? (section[key] as boolean) : undefined

  return (
    <SectionCard
      title="UC tagging"
      description="Apply declared Unity Catalog tags to generated tables after deploys."
      presence={{
        present,
        onEnable: () => form.set([...BASE], { enabled: true }),
        onDisable: () => form.del([...BASE]),
        confirmText: 'Removes the whole uc_tagging section from lhp.yaml.',
      }}
    >
      <SectionIssues issues={issuesAtExactly(form.issues, [...BASE])} />
      {!broken && (
        <>
          <BoolSwitch
            id="uc-tagging-enabled"
            label="Enabled"
            value={strictBool('enabled')}
            defaultValue={true}
            onSet={(v) => form.setField([...BASE], 'enabled', v)}
            onReset={() => form.del([...BASE, 'enabled'])}
            issue={issueText(form.issues, [...BASE, 'enabled'])?.message}
          />
          <BoolSwitch
            id="uc-tagging-remove-undeclared"
            label="Remove undeclared tags"
            description="Also delete tags that exist in UC but not in the YAML."
            value={strictBool('remove_undeclared_tags')}
            defaultValue={false}
            onSet={(v) => form.setField([...BASE], 'remove_undeclared_tags', v)}
            onReset={() => form.del([...BASE, 'remove_undeclared_tags'])}
            issue={issueText(form.issues, [...BASE, 'remove_undeclared_tags'])?.message}
          />
          <OptionalNumberField
            id="uc-tagging-concurrency"
            label="Tag update concurrency"
            value={section.tag_update_concurrency}
            min={1}
            max={20}
            onSet={(v) => form.setField([...BASE], 'tag_update_concurrency', v)}
            onUnset={() => form.del([...BASE, 'tag_update_concurrency'])}
            placeholder="default: 16"
            issue={issueText(form.issues, [...BASE, 'tag_update_concurrency'])?.message}
          />
        </>
      )}
    </SectionCard>
  )
}
