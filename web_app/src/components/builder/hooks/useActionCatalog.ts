import type { ActionCatalogEntry } from '../types/builder'

export const ACTION_CATALOG: ActionCatalogEntry[] = [
  // Load actions
  { type: 'load', subtype: 'cloudfiles', label: 'CloudFiles', description: 'Load from cloud storage (Auto Loader)', hasMVPForm: true },
  { type: 'load', subtype: 'delta', label: 'Delta', description: 'Read from a Delta table', hasMVPForm: true },
  { type: 'load', subtype: 'sql', label: 'SQL', description: 'Load via SQL query', hasMVPForm: true },
  { type: 'load', subtype: 'jdbc', label: 'JDBC', description: 'Load from external database', hasMVPForm: false },
  { type: 'load', subtype: 'python', label: 'Python', description: 'Custom Python loader', hasMVPForm: true },
  { type: 'load', subtype: 'custom_datasource', label: 'Custom DataSource', description: 'Custom Spark data source', hasMVPForm: true },
  { type: 'load', subtype: 'kafka', label: 'Kafka', description: 'Stream from Kafka', hasMVPForm: false },

  // Transform actions
  { type: 'transform', subtype: 'sql_transform', label: 'SQL', description: 'SQL transformation', hasMVPForm: true },
  { type: 'transform', subtype: 'python_transform', label: 'Python', description: 'Python transformation', hasMVPForm: true },
  { type: 'transform', subtype: 'data_quality', label: 'Data Quality', description: 'Expectations and quality rules', hasMVPForm: true },
  { type: 'transform', subtype: 'temp_table', label: 'Temp Table', description: 'Temporary view for intermediate results', hasMVPForm: true },
  { type: 'transform', subtype: 'schema', label: 'Schema', description: 'Schema enforcement/evolution', hasMVPForm: true },

  // Write actions
  { type: 'write', subtype: 'streaming_table', label: 'Streaming Table', description: 'Write to a streaming table', hasMVPForm: true },
  { type: 'write', subtype: 'materialized_view', label: 'Materialized View', description: 'Create a materialized view', hasMVPForm: true },
  { type: 'write', subtype: 'sink', label: 'Sink', description: 'Write to external destination', hasMVPForm: false },
]

export const ACTION_TYPE_COLORS: Record<string, { bg: string; border: string; text: string }> = {
  load: { bg: 'bg-blue-50', border: 'border-blue-400', text: 'text-blue-700' },
  transform: { bg: 'bg-emerald-50', border: 'border-emerald-400', text: 'text-emerald-700' },
  write: { bg: 'bg-orange-50', border: 'border-orange-400', text: 'text-orange-700' },
}

export function getCatalogByType(type: string): ActionCatalogEntry[] {
  return ACTION_CATALOG.filter((a) => a.type === type)
}
