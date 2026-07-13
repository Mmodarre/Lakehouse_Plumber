import { FilePlus2, Loader2, SquarePen } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { OptionalTextField } from '@/components/config/fields/OptionalTextField'
import type { CodeFieldInfo } from './codeFields'
import type { CodeTarget } from './CodeModal'
import type { FieldSpec } from './specs/types'
import { companionCheckablePath, useCompanionFile } from './useCompanionFile'

// ── ActionFormCodeField — code affordances for SQL/Python fields ─
//
// Rendered by the ActionForm engine for fields `codeFieldForPath` marks as
// code. Inline bodies (`sql`) get an "Edit in editor" button that opens the
// Monaco modal on the value; file refs (`sql_path` / `module_path` /
// `expectations_file`) get existence-aware actions — "Edit file" when it
// exists, "Create file" (writes a stub, then opens the modal) when it does
// not. Both mutate, so they honor the enforcing dirty-buffer guard: when
// `disabled`, editing opens read-only and Create is withheld.

export interface CodeFieldRowProps {
  id: string
  field: FieldSpec
  code: CodeFieldInfo
  value: unknown
  actionId: string
  issue: string | undefined
  disabled: boolean
  onSet: (val: unknown) => void
  onUnset: () => void
  onEditCode: (target: CodeTarget) => void
}

export function CodeFieldRow(props: CodeFieldRowProps) {
  return props.code.backing === 'inline' ? (
    <InlineCodeField {...props} />
  ) : (
    <CompanionFileField {...props} />
  )
}

function InlineCodeField({
  id,
  field,
  code,
  value,
  actionId,
  issue,
  disabled,
  onSet,
  onUnset,
  onEditCode,
}: CodeFieldRowProps) {
  return (
    <div className="space-y-1.5">
      <OptionalTextField
        id={id}
        label={field.label}
        value={value}
        onSet={onSet}
        onUnset={onUnset}
        helpPath={field.path}
        placeholder={field.placeholder}
        monospace={field.monospace}
        multiline
        issue={issue}
        issueSeverity="warning"
        disabled={disabled}
      />
      <div className="flex justify-end">
        <Button
          type="button"
          variant="ghost"
          size="xs"
          onClick={() =>
            onEditCode({
              backing: 'inline',
              actionId,
              path: field.path,
              title: field.label,
              language: code.inlineLanguage,
              initialValue: typeof value === 'string' ? value : '',
            })
          }
        >
          <SquarePen aria-hidden="true" />
          Edit in editor
        </Button>
      </div>
    </div>
  )
}

function stubFor(path: string): string {
  const ext = path.split('.').pop()?.toLowerCase() ?? ''
  switch (ext) {
    case 'sql':
      return '-- SQL\n'
    case 'py':
      return '# Python\n'
    case 'json':
      return '{}\n'
    case 'yaml':
    case 'yml':
      return '# Expectations\n'
    default:
      return '\n'
  }
}

function CompanionFileField({
  id,
  field,
  value,
  issue,
  disabled,
  onSet,
  onUnset,
  onEditCode,
}: CodeFieldRowProps) {
  const path = companionCheckablePath(value)
  const companion = useCompanionFile(path)

  const openEditor = () => {
    if (path === null) return
    onEditCode({ backing: 'file', title: field.label, filePath: path })
  }

  const handleCreate = async () => {
    if (path === null) return
    const created = await companion.create(stubFor(path))
    if (created) openEditor()
  }

  return (
    <div className="space-y-1.5">
      <OptionalTextField
        id={id}
        label={field.label}
        value={value}
        onSet={onSet}
        onUnset={onUnset}
        helpPath={field.path}
        placeholder={field.placeholder}
        monospace={field.monospace}
        issue={issue}
        issueSeverity="warning"
        disabled={disabled}
      />
      {path !== null && companion.status === 'exists' && (
        <div className="flex justify-end">
          <Button type="button" variant="ghost" size="xs" onClick={openEditor}>
            <SquarePen aria-hidden="true" />
            Edit file
          </Button>
        </div>
      )}
      {path !== null && companion.status === 'missing' && (
        <div className="flex items-center justify-between gap-2 rounded-sm border border-dashed border-border px-2 py-1.5">
          <span className="text-2xs text-muted-foreground">This file doesn&apos;t exist yet.</span>
          {disabled ? (
            <span className="text-2xs text-muted-foreground">Save text edits to create it.</span>
          ) : (
            <Button type="button" variant="outline" size="xs" onClick={() => void handleCreate()}>
              <FilePlus2 aria-hidden="true" />
              Create file
            </Button>
          )}
        </div>
      )}
      {path !== null && companion.status === 'checking' && (
        <div className="flex items-center gap-1 text-2xs text-muted-foreground">
          <Loader2 className="size-3 animate-spin" aria-hidden="true" />
          Checking…
        </div>
      )}
    </div>
  )
}
