import { isPlainObject } from '../../../lib/config-model'
import { SectionCard } from '../SectionCard'
import { OptionalTextField } from '../fields/OptionalTextField'
import { SectionIssues } from './SectionIssues'
import type { ProjectFormApi } from './projectFormSupport'
import { issueText, issuesAtExactly } from './projectFormSupport'

// test_reporting (loaders/_test_reporting_config_parser.py) and wheel
// (models/_project.py WheelConfig) — two small optional sections rendered
// as sibling cards. Skeletons are {} — test_reporting's two required
// fields then surface as a section error prompting the user to fill them.

export function TestReportingWheelSection({ form }: { form: ProjectFormApi }) {
  return (
    <>
      <TestReportingCard form={form} />
      <WheelCard form={form} />
    </>
  )
}

const TR_BASE = ['test_reporting'] as const

function TestReportingCard({ form }: { form: ProjectFormApi }) {
  const raw = form.doc.test_reporting
  const present = 'test_reporting' in form.doc
  const section = isPlainObject(raw) ? raw : {}
  const broken = present && !isPlainObject(raw)

  const fields: [string, string][] = [
    ['module_path', 'Module path'],
    ['function_name', 'Function name'],
    ['config_file', 'Config file'],
  ]

  return (
    <SectionCard
      title="Test reporting"
      description="Custom hook invoked with the results of generated test actions."
      presence={{
        present,
        onEnable: () => form.set([...TR_BASE], {}),
        onDisable: () => form.del([...TR_BASE]),
        confirmText: 'Removes the whole test_reporting section from lhp.yaml.',
      }}
    >
      <SectionIssues issues={issuesAtExactly(form.issues, [...TR_BASE])} />
      {!broken &&
        fields.map(([key, label]) => (
          <OptionalTextField
            key={key}
            id={`test-reporting-${key}`}
            label={label}
            value={section[key]}
            onSet={(v) => form.setField([...TR_BASE], key, v)}
            onUnset={() => form.del([...TR_BASE, key])}
            monospace
            helpPath={[...TR_BASE, key]}
            issue={issueText(form.issues, [...TR_BASE, key])?.message}
          />
        ))}
    </SectionCard>
  )
}

const WHEEL_BASE = ['wheel'] as const

function WheelCard({ form }: { form: ProjectFormApi }) {
  const raw = form.doc.wheel
  const present = 'wheel' in form.doc
  const section = isPlainObject(raw) ? raw : {}
  const broken = present && !isPlainObject(raw)

  return (
    <SectionCard
      title="Wheel packaging"
      description="Ship project Python code as a wheel with generated pipelines."
      presence={{
        present,
        onEnable: () => form.set([...WHEEL_BASE], {}),
        onDisable: () => form.del([...WHEEL_BASE]),
        confirmText: 'Removes the whole wheel section from lhp.yaml.',
      }}
    >
      <SectionIssues issues={issuesAtExactly(form.issues, [...WHEEL_BASE])} />
      {!broken && (
        <OptionalTextField
          id="wheel-artifact-volume"
          label="Artifact volume"
          value={section.artifact_volume}
          onSet={(v) => form.setField([...WHEEL_BASE], 'artifact_volume', v)}
          onUnset={() => form.del([...WHEEL_BASE, 'artifact_volume'])}
          monospace
          helpPath={['wheel', 'artifact_volume']}
          issue={issueText(form.issues, [...WHEEL_BASE, 'artifact_volume'])?.message}
        />
      )}
    </SectionCard>
  )
}
