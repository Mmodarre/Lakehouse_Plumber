// ── Action spec registry — lookup by (kind, subType) ─────────
//
// The single place the engine resolves a spec. A sub-type with no spec here
// returns `undefined`, and the inspector falls back to the read-only summary
// plus an "edit the YAML" note. Adding a form is one import + one line — no
// engine change.

import type { ActionKind, ActionSubTypeSpec } from './types'
import { loadCloudfilesSpec } from './load-cloudfiles'
import { loadCustomDatasourceSpec } from './load-custom-datasource'
import { loadDeltaSpec } from './load-delta'
import { loadJdbcSpec } from './load-jdbc'
import { loadKafkaSpec } from './load-kafka'
import { loadPythonSpec } from './load-python'
import { loadSqlSpec } from './load-sql'
import { transformDataQualitySpec } from './transform-data-quality'
import { transformPythonSpec } from './transform-python'
import { transformSchemaSpec } from './transform-schema'
import { transformSqlSpec } from './transform-sql'
import { transformTempTableSpec } from './transform-temp-table'
import { writeStreamingTableSpec } from './write-streaming-table'
import { writeMaterializedViewSpec } from './write-materialized-view'
import { writeSinkSpec } from './write-sink'
import { testRowCountSpec } from './test-row-count'
import { testUniquenessSpec } from './test-uniqueness'
import { testReferentialIntegritySpec } from './test-referential-integrity'
import { testCompletenessSpec } from './test-completeness'
import { testRangeSpec } from './test-range'
import { testSchemaMatchSpec } from './test-schema-match'
import { testAllLookupsFoundSpec } from './test-all-lookups-found'
import { testCustomSqlSpec } from './test-custom-sql'
import { testCustomExpectationsSpec } from './test-custom-expectations'

const SPECS: readonly ActionSubTypeSpec[] = [
  loadCloudfilesSpec,
  loadDeltaSpec,
  loadSqlSpec,
  loadPythonSpec,
  loadJdbcSpec,
  loadCustomDatasourceSpec,
  loadKafkaSpec,
  transformSqlSpec,
  transformPythonSpec,
  transformDataQualitySpec,
  transformTempTableSpec,
  transformSchemaSpec,
  writeStreamingTableSpec,
  writeMaterializedViewSpec,
  writeSinkSpec,
  testRowCountSpec,
  testUniquenessSpec,
  testReferentialIntegritySpec,
  testCompletenessSpec,
  testRangeSpec,
  testSchemaMatchSpec,
  testAllLookupsFoundSpec,
  testCustomSqlSpec,
  testCustomExpectationsSpec,
]

const key = (kind: ActionKind, subType: string): string => `${kind}/${subType}`

const REGISTRY: ReadonlyMap<string, ActionSubTypeSpec> = new Map(
  SPECS.map((spec) => [key(spec.kind, spec.subType), spec]),
)

/** The spec for an action sub-type, or `undefined` when no form exists yet. */
export function getActionSpec(
  kind: ActionKind | undefined,
  subType: string | undefined,
): ActionSubTypeSpec | undefined {
  if (kind === undefined || subType === undefined) return undefined
  return REGISTRY.get(key(kind, subType))
}

/**
 * Every registered spec, in registry order — the source of truth for the
 * add-action palette (each spec already carries `kind` / `subType` / `title`
 * / `summary`, so the palette needs no separate label map).
 */
export function listActionSpecs(): readonly ActionSubTypeSpec[] {
  return SPECS
}
