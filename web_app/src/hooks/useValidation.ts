import { useMutation } from '@tanstack/react-query'
import { triggerValidation } from '../api/validation'

export function useValidation() {
  return useMutation({
    mutationFn: ({ env, pipeline }: { env: string; pipeline?: string }) =>
      triggerValidation(env, pipeline),
  })
}
