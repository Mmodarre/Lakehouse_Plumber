import { SectionCard } from '../SectionCard'
import { StringListEditor } from '../fields/StringListEditor'
import type { ProjectFormApi } from './projectFormSupport'
import { issueText } from './projectFormSupport'

// include / blueprint_include / instance_include — top-level glob-pattern
// lists (loaders/_include_patterns_parser.py). Removing the last pattern
// deletes the key: an absent list and an empty list mean the same thing
// to the loader, and absence is the pristine form.

const LISTS: [key: string, label: string, description: string][] = [
  ['include', 'Include patterns', 'Flowgroup YAML files to include, relative to pipelines/.'],
  ['blueprint_include', 'Blueprint include patterns', 'Blueprint definition files to include.'],
  ['instance_include', 'Instance include patterns', 'Blueprint instance files to include.'],
]

export function IncludesSection({ form }: { form: ProjectFormApi }) {
  return (
    <SectionCard
      title="Includes"
      description="Glob patterns selecting which YAML files each generate run reads."
    >
      {LISTS.map(([key, label, description]) => {
        const raw = form.doc[key]
        const value = Array.isArray(raw) ? raw : undefined
        return (
          <StringListEditor
            key={key}
            id={`project-${key}`}
            label={label}
            description={description}
            value={key in form.doc ? value : undefined}
            monospace
            placeholder="e.g. bronze_*.yaml"
            onEditItem={(i, v) => form.set([key, i], v)}
            onAddItem={(v) =>
              value === undefined ? form.set([key], [v]) : form.set([key, value.length], v)
            }
            onRemoveItem={(i) => form.del([key, i])}
            onDeleteKey={() => form.del([key])}
            issue={issueText(form.issues, [key])?.message}
            itemIssue={(i) => issueText(form.issues, [key, i])?.message}
          />
        )
      })}
    </SectionCard>
  )
}
