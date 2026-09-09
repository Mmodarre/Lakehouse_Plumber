import type { TemplateAuthoringParameter } from '@/api/template-authoring'

export function missingTemplateParameters(params: readonly TemplateAuthoringParameter[], values: Record<string, unknown>): string[] {
  return params.filter((param) => param.required && !Object.hasOwn(values, param.name)).map((param) => param.name)
}
