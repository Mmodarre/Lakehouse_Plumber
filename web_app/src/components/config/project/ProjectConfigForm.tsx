import { useMemo } from 'react'
import { TriangleAlert } from 'lucide-react'
import { EmptyState } from '../../common/EmptyState'
import { SkeletonLoader } from '../../common/SkeletonLoader'
import type { UseConfigFileResult } from '../../../hooks/useConfigFile'
import {
  isPlainObject,
  listProjectPassthroughKeys,
  validateProjectConfig,
} from '../../../lib/config-model'
import { documentCount, toJS } from '../../../lib/yaml-doc'
import { ConfigConflictDialog } from '../ConfigConflictDialog'
import { ConfigPageShell } from '../ConfigPageShell'
import { ExternalChangeBanner } from '../ExternalChangeBanner'
import { ParseErrorsCard } from '../ParseErrorsCard'
import { PassthroughKeysCard } from '../PassthroughKeysCard'
import { SaveBar } from '../SaveBar'
import { EventLogSection } from './EventLogSection'
import { GeneralSection } from './GeneralSection'
import { IncludesSection } from './IncludesSection'
import { MonitoringSection } from './MonitoringSection'
import { OperationalMetadataSection } from './OperationalMetadataSection'
import { SandboxSection } from './SandboxSection'
import { SectionIssues } from './SectionIssues'
import { TestReportingWheelSection } from './TestReportingWheelSection'
import { UcTaggingSection } from './UcTaggingSection'
import { buildProjectFormApi, countErrors, issuesAtExactly } from './projectFormSupport'

// ── ProjectConfigForm — the lhp.yaml editor ──────────────────
//
// Owns the whole project tab surface around one useConfigFile('lhp.yaml')
// instance: shell + section cards + SaveBar + conflict/external-change
// wiring. Section order mirrors PROJECT_OPTIONAL_SECTIONS. All writes are
// surgical (setPath/deletePath through the hook's mutate) — YAML the user
// did not touch is never normalized, and Save is disabled while any
// config-model ERROR exists (the CLI loader would reject the file).

export interface ProjectConfigFormProps {
  /** A useConfigFile('lhp.yaml') instance owned by the page. */
  file: UseConfigFileResult
}

export function ProjectConfigForm({ file }: ProjectConfigFormProps) {
  const parsed = file.handle !== null && file.errors.length === 0

  // The handle is mutable: derive ONLY from [handle, version] (hook contract).
  const { docRaw, docCount } = useMemo(() => {
    if (!parsed || file.handle === null) return { docRaw: undefined, docCount: 0 }
    const count = documentCount(file.handle)
    return { docRaw: count > 0 ? toJS(file.handle, 0) : undefined, docCount: count }
    // eslint-disable-next-line react-hooks/exhaustive-deps -- version IS the handle's change signal
  }, [file.handle, file.version, parsed])

  const issues = useMemo(() => (parsed ? validateProjectConfig(docRaw) : []), [docRaw, parsed])
  // A non-mapping document (e.g. a top-level list) cannot back the form.
  const doc = isPlainObject(docRaw) ? docRaw : undefined
  const unmapped = docRaw !== undefined && docRaw !== null && doc === undefined
  const passthroughKeys = useMemo(
    () => (doc !== undefined ? listProjectPassthroughKeys(doc) : []),
    [doc],
  )
  const form = buildProjectFormApi(file, doc ?? {}, issues)
  const errorCount = file.errors.length + countErrors(issues)

  let body: React.ReactNode
  if (file.isLoading) {
    body = <SkeletonLoader lines={6} />
  } else if (file.loadError !== null) {
    body = <EmptyState title="Failed to load file" message={file.loadError} icon={TriangleAlert} />
  } else if (file.handle === null) {
    body = null
  } else if (file.errors.length > 0) {
    body = <ParseErrorsCard errors={file.errors} />
  } else {
    body = (
      <>
        {file.yamlError && (
          <p role="alert" className="text-xs text-destructive">
            Saved with a YAML syntax error (line {file.yamlError.line}, column{' '}
            {file.yamlError.column}): {file.yamlError.message}
          </p>
        )}
        {docCount > 1 && (
          <p className="text-2xs text-muted-foreground">
            This file contains {docCount} YAML documents — the form edits the first one.
          </p>
        )}
        <SectionIssues issues={issuesAtExactly(issues, [])} />
        {!unmapped && (
          <>
            <GeneralSection form={form} />
            <IncludesSection form={form} />
            <OperationalMetadataSection form={form} />
            <EventLogSection form={form} />
            <MonitoringSection form={form} />
            <UcTaggingSection form={form} />
            <TestReportingWheelSection form={form} />
            <SandboxSection form={form} />
            <PassthroughKeysCard keys={passthroughKeys} />
          </>
        )}
      </>
    )
  }

  return (
    <>
      <ConfigPageShell
        title="Project configuration"
        description="lhp.yaml — project-wide settings applied to every generate."
        banner={
          file.externalChange ? (
            <ExternalChangeBanner onReload={() => void file.reload()} onKeep={file.keepMine} />
          ) : undefined
        }
        footer={
          <SaveBar
            path={file.path}
            dirty={file.dirty}
            saving={file.saving}
            errorCount={errorCount}
            onSave={() => void file.save()}
          />
        }
      >
        {body}
      </ConfigPageShell>
      <ConfigConflictDialog
        path={file.conflict ? file.path : null}
        saving={file.saving}
        onReload={() => void file.reload()}
        onOverwrite={() => void file.overwrite()}
        onCancel={file.dismissConflict}
      />
    </>
  )
}
