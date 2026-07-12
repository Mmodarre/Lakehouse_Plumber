import { fetchApi } from './client'
import type { SandboxScope } from '../types/api'

// `GET /api/sandbox` → the `.lhp/profile.yaml` selection and the concrete
// pipeline scope `--sandbox` would run.
//
// `env` is OMITTED entirely for scope display: an empty `?env=` is read as a
// literal (invalid) environment and trips the allowed-envs gate (LHP-CFG-065).
// Pass an env only to apply that gate for a specific environment.
export function fetchSandboxScope(env?: string): Promise<SandboxScope> {
  const params = env ? `?env=${encodeURIComponent(env)}` : ''
  return fetchApi(`/sandbox${params}`)
}
