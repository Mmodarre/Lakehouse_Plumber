import type { ActionNodeConfig } from '../types/builder'
import { CloudFilesLoadForm } from './forms/CloudFilesLoadForm'
import { DeltaLoadForm } from './forms/DeltaLoadForm'
import { SQLLoadForm } from './forms/SQLLoadForm'
import { PythonLoadForm } from './forms/PythonLoadForm'
import { CustomDataSourceLoadForm } from './forms/CustomDataSourceLoadForm'
import { SQLTransformForm } from './forms/SQLTransformForm'
import { PythonTransformForm } from './forms/PythonTransformForm'
import { DataQualityTransformForm } from './forms/DataQualityTransformForm'
import { TempTableTransformForm } from './forms/TempTableTransformForm'
import { SchemaTransformForm } from './forms/SchemaTransformForm'
import { StreamingTableWriteForm } from './forms/StreamingTableWriteForm'
import { MaterializedViewWriteForm } from './forms/MaterializedViewWriteForm'
import { GenericYAMLActionForm } from './forms/GenericYAMLActionForm'

interface ActionFormRouterProps {
  config: ActionNodeConfig
  onChange: (updates: Partial<ActionNodeConfig['config']>) => void
  upstreamNames: string[]
  target: string
  onTargetChange: (target: string) => void
}

export function ActionFormRouter({ config, onChange, upstreamNames, target, onTargetChange }: ActionFormRouterProps) {
  const key = `${config.actionType}:${config.actionSubtype}`

  switch (key) {
    // Load actions
    case 'load:cloudfiles':
      return <CloudFilesLoadForm config={config.config} onChange={onChange} target={target} onTargetChange={onTargetChange} />
    case 'load:delta':
      return <DeltaLoadForm config={config.config} onChange={onChange} target={target} onTargetChange={onTargetChange} />
    case 'load:sql':
      return <SQLLoadForm config={config.config} onChange={onChange} target={target} onTargetChange={onTargetChange} />
    case 'load:python':
      return <PythonLoadForm config={config.config} onChange={onChange} target={target} onTargetChange={onTargetChange} />
    case 'load:custom_datasource':
      return <CustomDataSourceLoadForm config={config.config} onChange={onChange} target={target} onTargetChange={onTargetChange} />

    // Transform actions
    case 'transform:sql_transform':
      return <SQLTransformForm config={config.config} onChange={onChange} upstreamNames={upstreamNames} target={target} onTargetChange={onTargetChange} />
    case 'transform:python_transform':
      return <PythonTransformForm config={config.config} onChange={onChange} upstreamNames={upstreamNames} target={target} onTargetChange={onTargetChange} />
    case 'transform:data_quality':
      return <DataQualityTransformForm config={config.config} onChange={onChange} upstreamNames={upstreamNames} target={target} onTargetChange={onTargetChange} />
    case 'transform:temp_table':
      return <TempTableTransformForm config={config.config} onChange={onChange} upstreamNames={upstreamNames} target={target} onTargetChange={onTargetChange} />
    case 'transform:schema':
      return <SchemaTransformForm config={config.config} onChange={onChange} upstreamNames={upstreamNames} target={target} onTargetChange={onTargetChange} />

    // Write actions
    case 'write:streaming_table':
      return <StreamingTableWriteForm config={config.config} onChange={onChange} upstreamNames={upstreamNames} />
    case 'write:materialized_view':
      return <MaterializedViewWriteForm config={config.config} onChange={onChange} upstreamNames={upstreamNames} />

    default:
      return <GenericYAMLActionForm config={config} />
  }
}
