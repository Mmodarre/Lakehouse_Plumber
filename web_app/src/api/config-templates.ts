import { fetchApiTextWithMeta } from './client'

/** Template kinds served by `GET /api/config-templates/{kind}`. */
export type ConfigTemplateKind = 'pipeline_config' | 'job_config' | 'monitoring_job_config'

/**
 * `GET /api/config-templates/{kind}` → the raw packaged template text
 * (text/plain, no envelope), used directly as initial file content by
 * the create-from-template dialog.
 */
export async function fetchConfigTemplate(kind: ConfigTemplateKind): Promise<string> {
  const { content } = await fetchApiTextWithMeta(`/config-templates/${kind}`)
  return content
}
