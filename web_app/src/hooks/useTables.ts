import { useQuery } from '@tanstack/react-query'
import { fetchTables } from '../api/tables'

export function useTables(env: string, pipeline?: string) {
  return useQuery({
    queryKey: ['tables', env, pipeline],
    queryFn: () => fetchTables(env, pipeline),
    enabled: !!env,
  })
}
