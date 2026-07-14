import { useMemo } from 'react'
import { TriangleAlert } from 'lucide-react'
import { EmptyState } from '../../common/EmptyState'
import { SchemaKindProvider } from '../../common/SchemaKindContext'
import { SkeletonLoader } from '../../common/SkeletonLoader'
import {
  isPlainObject,
  listProjectPassthroughKeys,
  validateProjectConfig,
} from '../../../lib/config-model'
import { documentCount, toJS } from '../../../lib/yaml-doc'
import { ParseErrorsCard } from '../ParseErrorsCard'
import { PassthroughKeysCard } from '../PassthroughKeysCard'
import type { ConfigDocSource } from '../shared/docFormSupport'
import { EventLogSection } from './EventLogSection'
import { GeneralSection } from './GeneralSection'
import { IncludesSection } from './IncludesSection'
import { MonitoringSection } from './MonitoringSection'
import { OperationalMetadataSection } from './OperationalMetadataSection'
import { SandboxSection } from './SandboxSection'
import { SectionIssues } from './SectionIssues'
import { TestReportingWheelSection } from './TestReportingWheelSection'
import { UcTaggingSection } from './UcTaggingSection'
import { buildProjectFormApi, issuesAtExactly } from './projectFormSupport'

// ── ProjectConfigForm — the lhp.yaml editor body ─────────────
//
// The section-card body of the project (lhp.yaml) config surface, re-hosted
// by components/entity/ConfigFormView (which owns the scroll frame, degraded
// banner, and viewer read-only wrapper). Section order mirrors
// PROJECT_OPTIONAL_SECTIONS. All writes are surgical (setPath/deletePath
// through the source's mutate) — YAML the user did not touch is never
// normalized. Config-model validator issues render per field; they no longer
// block anything (saving is text-canonical via the buffer's ⌘S path).

export interface ProjectConfigFormProps {
  /** The lhp.yaml config document source (documentStore-backed). */
  file: ConfigDocSource
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

  return <SchemaKindProvider kind="project">{body}</SchemaKindProvider>
}
