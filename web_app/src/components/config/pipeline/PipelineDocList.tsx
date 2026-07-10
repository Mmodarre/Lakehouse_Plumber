import { Boxes, FileQuestion, Layers, SlidersHorizontal, Workflow } from 'lucide-react'
import { AddButton, Connector, RailRow } from '../shared/Rail'
import type { RailSelection } from '../shared/docFormSupport'
import type { RailDoc } from './pipelineFormSupport'

// ── PipelineDocList — the precedence rail ────────────────────
//
// Master list of the file's documents, arranged as the merge cascade the
// loader actually applies: Built-in defaults → Project defaults → the
// per-pipeline documents. The connector between the first two tiers and
// the docs SECTION teaches "later overrides earlier"; the documents
// inside the section are deliberately rendered as PEERS on one spine
// (fan-out), because groups and singles are ONE precedence tier — each
// pipeline merges built-in → project defaults → its own document, and a
// name appearing in two documents is an error, never a chain.
//
// Rows carry error/warning counts from validatePipelineConfigFile and a
// "duplicate" badge on EVERY document involved in a VAL_006 clash. The
// row/connector/add-affordance pieces live in ../shared/Rail (shared with
// the job editor); only the tier composition here is pipeline-specific.

export interface PipelineDocListProps {
  rail: RailDoc[]
  selected: RailSelection
  onSelect: (selection: RailSelection) => void
  /** Doc management enabled (file parsed cleanly). */
  canEdit: boolean
  onAddSingle: () => void
  onAddGroup: () => void
  onAddDefaults: () => void
}

export function PipelineDocList({
  rail,
  selected,
  onSelect,
  canEdit,
  onAddSingle,
  onAddGroup,
  onAddDefaults,
}: PipelineDocListProps) {
  const defaultsDocs = rail.filter((doc) => doc.kind === 'defaults')
  const otherDocs = rail.filter((doc) => doc.kind !== 'defaults')

  return (
    <nav aria-label="Configuration documents" className="w-60 shrink-0">
      <p className="mb-2 text-2xs font-medium uppercase tracking-wide text-muted-foreground">
        Precedence — lowest first
      </p>

      {/* Tier 1: built into LHP, read-only. */}
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

      {/* Tier 3: per-pipeline documents — PEERS on one spine (fan-out),
          never a chain. */}
      <div className="ml-4 border-l border-border pl-2">
        <p className="px-2 pb-1 text-2xs text-muted-foreground">
          Per-pipeline documents — one tier: each pipeline merges built-in →
          project defaults → its own document.
        </p>
        <div className="space-y-0.5">
          {otherDocs.map((doc) => (
            <RailRow
              key={doc.index}
              icon={doc.kind === 'unrecognized' ? FileQuestion : doc.isGroup ? Boxes : Workflow}
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
            <AddButton label="Add pipeline" onClick={onAddSingle} />
            <AddButton label="Add group" onClick={onAddGroup} />
          </div>
        )}
      </div>
    </nav>
  )
}
