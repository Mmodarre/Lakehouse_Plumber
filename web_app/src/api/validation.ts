import { fetchApi } from './client'
import type { ValidateResponse } from '../types/api'

export function triggerValidation(
  env: string,
  pipeline?: string,
): Promise<ValidateResponse> {
  return fetchApi('/validate', {
    method: 'POST',
    body: JSON.stringify({
      environment: env,
      ...(pipeline && { pipeline }),
    }),
  })
}

export function validatePipeline(
  name: string,
  env: string,
): Promise<ValidateResponse> {
  return fetchApi(`/validate/pipeline/${encodeURIComponent(name)}`, {
    method: 'POST',
    body: JSON.stringify({ environment: env }),
  })
}
