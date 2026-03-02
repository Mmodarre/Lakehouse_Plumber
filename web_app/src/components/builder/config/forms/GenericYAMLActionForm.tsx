import { useCallback, useMemo } from 'react'
import { stringify } from 'yaml'
import { Textarea } from '@/components/ui/textarea'
import { useBuilderStore } from '../../hooks/useBuilderStore'
import type { ActionNodeConfig } from '../../types/builder'

interface GenericYAMLActionFormProps {
  config: ActionNodeConfig
}

export function GenericYAMLActionForm({ config }: GenericYAMLActionFormProps) {
  const updateActionConfig = useBuilderStore((s) => s.updateActionConfig)

  const yamlContent = useMemo(() => {
    if (config.yamlOverride) return config.yamlOverride

    // Generate a skeleton YAML from the action type/subtype
    const skeleton: Record<string, unknown> = {
      name: config.actionName,
      type: config.actionType,
    }

    // Add subtype-specific skeleton matching LHP schema
    const key = `${config.actionType}:${config.actionSubtype}`
    switch (key) {
      case 'load:delta':
        skeleton.source = { type: 'delta', database: '', table: '' }
        break
      case 'load:sql':
        skeleton.source = { type: 'sql', sql: 'SELECT ...' }
        break
      case 'load:jdbc':
        skeleton.source = { type: 'jdbc', url: '', driver: '', query: '' }
        break
      case 'load:python':
        skeleton.source = { type: 'python', module_path: '', function_name: '' }
        break
      case 'load:custom_datasource':
        skeleton.source = { type: 'custom_datasource', module_path: '', custom_datasource_class: '' }
        break
      case 'load:kafka':
        skeleton.source = { type: 'kafka', bootstrap_servers: '', subscribe: '' }
        break
      case 'load:cloudfiles':
        skeleton.source = { type: 'cloudfiles', path: '', format: '' }
        break
      case 'transform:sql_transform':
        skeleton.transform_type = 'sql'
        skeleton.source = ''
        skeleton.sql = ''
        break
      case 'transform:python_transform':
        skeleton.transform_type = 'python'
        skeleton.source = ''
        skeleton.module_path = ''
        skeleton.function_name = ''
        break
      case 'transform:data_quality':
        skeleton.transform_type = 'data_quality'
        skeleton.source = ''
        skeleton.expectations_file = ''
        break
      case 'transform:temp_table':
        skeleton.transform_type = 'temp_table'
        skeleton.source = ''
        skeleton.target = 'temp_'
        break
      case 'transform:schema':
        skeleton.transform_type = 'schema'
        skeleton.source = ''
        skeleton.schema_file = ''
        skeleton.enforcement = ''
        break
      case 'write:streaming_table':
        skeleton.source = ''
        skeleton.write_target = { type: 'streaming_table', database: '', table: '' }
        break
      case 'write:materialized_view':
        skeleton.source = ''
        skeleton.write_target = { type: 'materialized_view', database: '', table: '' }
        break
      case 'write:sink':
        skeleton.source = ''
        skeleton.write_target = { type: 'sink', sink_type: 'delta', sink_name: '', options: {} }
        break
      default:
        // Unknown type — bare minimum
        if (config.actionType === 'load') {
          skeleton.source = { type: config.actionSubtype }
        } else if (config.actionType === 'transform') {
          skeleton.transform_type = config.actionSubtype
          skeleton.source = ''
        } else if (config.actionType === 'write') {
          skeleton.source = ''
          skeleton.write_target = { type: config.actionSubtype }
        }
        break
    }

    // Merge with existing config
    Object.assign(skeleton, config.config)

    return stringify(skeleton, { indent: 2 })
  }, [config])

  const handleChange = useCallback(
    (value: string) => {
      updateActionConfig(config.id, { yamlOverride: value })
    },
    [config.id, updateActionConfig],
  )

  return (
    <div className="space-y-2">
      <p className="text-xs text-slate-500">
        Edit the raw YAML for this action. The configuration will be included as-is in the generated flowgroup.
      </p>
      <Textarea
        value={yamlContent}
        onChange={(e) => handleChange(e.target.value)}
        className="min-h-[300px] font-mono text-xs"
        rows={15}
      />
    </div>
  )
}
