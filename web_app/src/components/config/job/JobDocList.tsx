import { Briefcase, Boxes, FileQuestion, Layers, SlidersHorizontal } from 'lucide-react'
import { AddButton, Connector, RailRow } from '../shared/Rail'
import type { RailSelection } from '../shared/docFormSupport'
import type { JobRailDoc } from './jobFormSupport'

// ── JobDocList — the job precedence rail ─────────────────────
//
// Same cascade the pipeline rail teaches, with the job loader's merge
// order: Built-in defaults (DEFAULT_JOB_CONFIG) → Project defaults → the
// per-job documents. Job documents are PEERS on one spine (fan-out) —
// each job merges built-in → project defaults → its own document, and a
// job_name appearing in two documents is an error (VAL_004), never a
// chain. Rows carry error/warning counts from validateJobConfigFile and
// a "duplicate" badge on EVERY document involved in a clash.
//
// Only STANDARD job_config files get this rail — monitoring configs are
// flat single-document files (no rail, no add affordances) and legacy
// flat files show the convert banner instead.

export interface JobDocListProps {
  rail: JobRailDoc[]
  selected: RailSelection
  onSelect: (selection: RailSelection) => void
  /** Doc management enabled (file parsed cleanly). */
  canEdit: boolean
  onAddSingle: () => void
  onAddGroup: () => void
  onAddDefaults: () => void
}

export function JobDocList({
  rail,
  selected,
  onSelect,
  canEdit,
  onAddSingle,
  onAddGroup,
  onAddDefaults,
}: JobDocListProps) {
  const defaultsDocs = rail.filter((doc) => doc.kind === 'defaults')
  const otherDocs = rail.filter((doc) => doc.kind !== 'defaults')

  return (
    <nav aria-label="Configuration documents" className="w-60 shrink-0">
      <p className="mb-2 text-2xs font-medium uppercase tracking-wide text-muted-foreground">
        Precedence — lowest first
      </p>

      {/* Tier 1: DEFAULT_JOB_CONFIG, read-only. */}
      <RailRow
        icon={Layers}
        label="Built-in defaults"
        caption="built into LHP"
        ghost
        selected={selected === 'builtin'}
        onClick={() => onSelect('builtin')}
      />
      <Connector />

      {/* Tier 2: the project_defaults document (or its add slot). */}
      {defaultsDocs.length === 0 ? (
        canEdit ? (
          <AddButton label="Add project defaults" onClick={onAddDefaults} />
        ) : (
          <p className="px-2 text-2xs text-muted-foreground">No project defaults</p>
        )
      ) : (
        defaultsDocs.map((doc) => (
          <RailRow
            key={doc.index}
            icon={SlidersHorizontal}
            label={doc.label}
            caption={doc.caption}
            selected={selected === doc.index}
            onClick={() => onSelect(doc.index)}
            errors={doc.errors}
            warnings={doc.warnings}
          />
        ))
      )}
      {defaultsDocs.length > 1 && (
        <p className="px-2 text-2xs text-warning">
          Multiple project_defaults documents — the loader uses the last one.
        </p>
      )}
      <Connector />

      {/* Tier 3: per-job documents — PEERS on one spine (fan-out). */}
      <div className="ml-4 border-l border-border pl-2">
        <p className="px-2 pb-1 text-2xs text-muted-foreground">
          Per-job documents — one tier: each job merges built-in → project
          defaults → its own document.
        </p>
        <div className="space-y-0.5">
          {otherDocs.map((doc) => (
            <RailRow
              key={doc.index}
              icon={doc.kind === 'unrecognized' ? FileQuestion : doc.isGroup ? Boxes : Briefcase}
              label={doc.label}
              caption={doc.caption}
              selected={selected === doc.index}
              onClick={() => onSelect(doc.index)}
              errors={doc.errors}
              warnings={doc.warnings}
              duplicate={doc.duplicate}
              ghost={doc.kind === 'unrecognized'}
            />
          ))}
        </div>
        {canEdit && (
          <div className="mt-1 flex flex-col items-start">
            <AddButton label="Add job" onClick={onAddSingle} />
            <AddButton label="Add job group" onClick={onAddGroup} />
          </div>
        )}
      </div>
    </nav>
  )
}
