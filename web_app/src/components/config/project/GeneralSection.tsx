import { parseLaxBool } from '../../../lib/config-model'
import { SectionCard } from '../SectionCard'
import { BoolSwitch } from '../fields/BoolSwitch'
import { OptionalTextField } from '../fields/OptionalTextField'
import type { ProjectFormApi } from './projectFormSupport'
import { issueText } from './projectFormSupport'

// General project identity — the top-level scalars of lhp.yaml
// (models/_project.py ProjectConfig). Always present as a card; each
// field is individually optional (empty commit deletes the key).

/** [key, label, mono, description?, placeholder?] per top-level string field. */
const TEXT_FIELDS: [string, string, boolean, string?, string?][] = [
  ['name', 'Name', true, undefined, 'unnamed_project'],
  ['version', 'Version', true, 'Quote it in YAML — 1.0 unquoted parses as a number.', '1.0'],
  ['description', 'Description', false],
  ['author', 'Author', false],
  ['created_date', 'Created date', true],
  [
    'required_lhp_version',
    'Required LHP version',
    true,
    'PEP 440 specifier the CLI enforces before generating (e.g. >=0.9,<1.0).',
  ],
]

export function GeneralSection({ form }: { form: ProjectFormApi }) {
  const applyFormatting = form.doc.apply_formatting
  return (
    <SectionCard title="General" description="Project identity and generation defaults.">
      {TEXT_FIELDS.map(([key, label, mono, description, placeholder]) => {
        const issue = issueText(form.issues, [key])
        return (
          <OptionalTextField
            key={key}
            id={`project-${key}`}
            label={label}
            value={form.doc[key]}
            onSet={(v) => form.set([key], v)}
            onUnset={() => form.del([key])}
            description={description}
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
        description="Run the code-formatting pass over generated Python files."
        value={'apply_formatting' in form.doc ? parseLaxBool(applyFormatting) : undefined}
        defaultValue={true}
        onSet={(v) => form.set(['apply_formatting'], v)}
        onReset={() => form.del(['apply_formatting'])}
        issue={issueText(form.issues, ['apply_formatting'])?.message}
      />
    </SectionCard>
  )
}
