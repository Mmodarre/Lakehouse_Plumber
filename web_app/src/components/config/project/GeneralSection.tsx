import { parseLaxBool } from '../../../lib/config-model'
import { SectionCard } from '../SectionCard'
import { BoolSwitch } from '../fields/BoolSwitch'
import { OptionalTextField } from '../fields/OptionalTextField'
import type { ProjectFormApi } from './projectFormSupport'
import { issueText } from './projectFormSupport'

// General project identity — the top-level scalars of lhp.yaml
// (models/_project.py ProjectConfig). Always present as a card; each
// field is individually optional (empty commit deletes the key).

/** [key, label, mono, placeholder?] per top-level string field. */
const TEXT_FIELDS: [string, string, boolean, string?][] = [
  ['name', 'Name', true, 'unnamed_project'],
  ['version', 'Version', true, '1.0'],
  ['description', 'Description', false],
  ['author', 'Author', false],
  ['created_date', 'Created date', true],
  ['required_lhp_version', 'Required LHP version', true],
]

export function GeneralSection({ form }: { form: ProjectFormApi }) {
  const applyFormatting = form.doc.apply_formatting
  return (
    <SectionCard title="General" description="Project identity and generation defaults.">
      {TEXT_FIELDS.map(([key, label, mono, placeholder]) => {
        const issue = issueText(form.issues, [key])
        return (
          <OptionalTextField
            key={key}
            id={`project-${key}`}
            label={label}
            value={form.doc[key]}
            onSet={(v) => form.set([key], v)}
            onUnset={() => form.del([key])}
            helpPath={[key]}
            placeholder={placeholder}
            monospace={mono}
            issue={issue?.message}
            issueSeverity={issue?.severity}
          />
        )
      })}
      <BoolSwitch
        id="project-apply-formatting"
        label="Apply formatting"
        helpPath={['apply_formatting']}
        value={'apply_formatting' in form.doc ? parseLaxBool(applyFormatting) : undefined}
        defaultValue={true}
        onSet={(v) => form.set(['apply_formatting'], v)}
        onReset={() => form.del(['apply_formatting'])}
        issue={issueText(form.issues, ['apply_formatting'])?.message}
      />
    </SectionCard>
  )
}
