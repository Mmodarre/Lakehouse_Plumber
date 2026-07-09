import { useState } from 'react'
import { Loader2, Settings2 } from 'lucide-react'
import { Button } from '../ui/button'
import { Input } from '../ui/input'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '../ui/select'
import {
  useDatabricksProfiles,
  useUpdateExecutorConfig,
} from '../../hooks/useAssistant'
import type {
  ExecutorConfig,
  ExecutorMode,
  ExecutorProvider,
} from '../../types/assistant'

// Mirrors the backend's env-var NAME validation (schemas/assistant.py):
// POSIX identifier only — pasted key material never passes.
const ENV_VAR_NAME_RE = /^[A-Za-z_][A-Za-z0-9_]*$/

const PROVIDER_LABELS: Record<ExecutorProvider, string> = {
  claude_sdk: 'Claude (built in)',
  omnigent: 'Omnigent daemon',
}

// Modes per provider, in display order; the first is the provider default.
const PROVIDER_MODES: Record<ExecutorProvider, ExecutorMode[]> = {
  claude_sdk: ['claude_subscription', 'databricks'],
  omnigent: ['omnigent_defaults', 'databricks', 'api_key_env'],
}

const MODE_LABELS: Record<ExecutorMode, string> = {
  claude_subscription: 'Claude subscription (Claude Code login)',
  omnigent_defaults: 'omnigent defaults',
  databricks: 'Databricks workspace',
  api_key_env: 'API key from environment variable',
}

/**
 * Provider + auth choice card. First use: shown until a config is stored,
 * blocking the composer (no `initial`, no `onDone`) — the PRODUCT default
 * `claude_sdk` is preselected. Reconfigure: opened from the panel's gear
 * button with the stored config prefilled; saving (or Cancel) returns to the
 * chat. Saving PUTs the config — the backend marks the active session stale,
 * so the next message reprovisions.
 */
export function SetupCard({
  initial,
  onDone,
}: {
  initial?: ExecutorConfig | null
  onDone?: () => void
}) {
  const [provider, setProvider] = useState<ExecutorProvider>(
    initial?.provider ?? 'claude_sdk',
  )
  const [mode, setMode] = useState<ExecutorMode>(
    initial?.mode ?? PROVIDER_MODES[initial?.provider ?? 'claude_sdk'][0],
  )
  const [profile, setProfile] = useState(initial?.profile ?? '')
  const [model, setModel] = useState(initial?.model ?? '')
  const [apiKeyEnv, setApiKeyEnv] = useState(initial?.api_key_env ?? '')
  const [oauthTokenEnv, setOauthTokenEnv] = useState(
    initial?.oauth_token_env ?? '',
  )

  const changeProvider = (next: ExecutorProvider) => {
    setProvider(next)
    setMode(PROVIDER_MODES[next][0])
  }

  const profiles = useDatabricksProfiles({ enabled: mode === 'databricks' })
  const update = useUpdateExecutorConfig()

  const valid =
    mode === 'omnigent_defaults' ||
    (mode === 'claude_subscription' &&
      (oauthTokenEnv === '' || ENV_VAR_NAME_RE.test(oauthTokenEnv))) ||
    (mode === 'databricks' && profile !== '') ||
    (mode === 'api_key_env' && ENV_VAR_NAME_RE.test(apiKeyEnv))

  const save = () => {
    update.mutate(
      {
        provider,
        mode,
        profile: mode === 'databricks' ? profile : null,
        host: null,
        model:
          mode !== 'omnigent_defaults' && model !== '' ? model : null,
        api_key_env: mode === 'api_key_env' ? apiKeyEnv : null,
        oauth_token_env:
          mode === 'claude_subscription' && oauthTokenEnv !== ''
            ? oauthTokenEnv
            : null,
      },
      { onSuccess: () => onDone?.() },
    )
  }

  return (
    <div className="p-3 text-xs text-muted-foreground">
      <div className="mb-1.5 flex items-center gap-1.5 text-sm font-medium text-foreground">
        <Settings2 className="size-4" aria-hidden="true" />
        Choose how the assistant runs
      </div>
      <p>
        One-time setup: pick the assistant provider and how it signs in. You
        can change it later from this panel.
      </p>

      <label
        className="mt-3 block font-medium text-foreground"
        htmlFor="assistant-provider"
      >
        Provider
      </label>
      <Select
        value={provider}
        onValueChange={(v) => changeProvider(v as ExecutorProvider)}
      >
        <SelectTrigger
          id="assistant-provider"
          size="sm"
          aria-label="Assistant provider"
          className="mt-1 w-full"
        >
          <SelectValue />
        </SelectTrigger>
        <SelectContent>
          {(Object.keys(PROVIDER_LABELS) as ExecutorProvider[]).map((p) => (
            <SelectItem key={p} value={p}>
              {PROVIDER_LABELS[p]}
            </SelectItem>
          ))}
        </SelectContent>
      </Select>
      {provider === 'claude_sdk' && (
        <p className="mt-1">
          Runs in-process — nothing else to install or start.
        </p>
      )}
      {provider === 'omnigent' && (
        <p className="mt-1">
          Uses a separately installed, user-managed local omnigent daemon.
        </p>
      )}

      <label
        className="mt-3 block font-medium text-foreground"
        htmlFor="assistant-mode"
      >
        {provider === 'claude_sdk' ? 'Sign in with' : 'Executor'}
      </label>
      <Select value={mode} onValueChange={(v) => setMode(v as ExecutorMode)}>
        <SelectTrigger
          id="assistant-mode"
          size="sm"
          aria-label="Auth mode"
          className="mt-1 w-full"
        >
          <SelectValue />
        </SelectTrigger>
        <SelectContent>
          {PROVIDER_MODES[provider].map((m) => (
            <SelectItem key={m} value={m}>
              {MODE_LABELS[m]}
            </SelectItem>
          ))}
        </SelectContent>
      </Select>

      {mode === 'claude_subscription' && (
        <>
          <label
            className="mt-3 block font-medium text-foreground"
            htmlFor="assistant-oauth-token-env"
          >
            Token environment variable{' '}
            <span className="font-normal text-muted-foreground">(optional)</span>
          </label>
          <Input
            id="assistant-oauth-token-env"
            value={oauthTokenEnv}
            onChange={(e) => setOauthTokenEnv(e.target.value)}
            placeholder="leave empty to use your Claude Code login"
            aria-invalid={
              oauthTokenEnv !== '' && !ENV_VAR_NAME_RE.test(oauthTokenEnv)
            }
            className="mt-1 h-8"
          />
          <p className="mt-1">
            Leave empty to use the machine's Claude Code sign-in. Or set the
            NAME of an environment variable holding a `claude setup-token`
            token — never paste the token itself.
          </p>
        </>
      )}

      {mode === 'databricks' && (
        <>
          <label
            className="mt-3 block font-medium text-foreground"
            htmlFor="assistant-profile"
          >
            Databricks profile
          </label>
          <Select value={profile} onValueChange={setProfile}>
            <SelectTrigger
              id="assistant-profile"
              size="sm"
              aria-label="Databricks profile"
              className="mt-1 w-full"
            >
              <SelectValue
                placeholder={
                  profiles.data && profiles.data.profiles.length === 0
                    ? 'No profiles in ~/.databrickscfg'
                    : 'Select a profile'
                }
              />
            </SelectTrigger>
            <SelectContent>
              {(profiles.data?.profiles ?? []).map((p) => (
                <SelectItem key={p} value={p}>
                  {p}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </>
      )}

      {mode === 'api_key_env' && (
        <>
          <label
            className="mt-3 block font-medium text-foreground"
            htmlFor="assistant-api-key-env"
          >
            Environment variable name
          </label>
          <Input
            id="assistant-api-key-env"
            value={apiKeyEnv}
            onChange={(e) => setApiKeyEnv(e.target.value)}
            placeholder="e.g. ANTHROPIC_API_KEY"
            aria-invalid={apiKeyEnv !== '' && !ENV_VAR_NAME_RE.test(apiKeyEnv)}
            className="mt-1 h-8"
          />
          <p className="mt-1">
            The NAME of an environment variable on the omnigent server —
            never paste the key itself.
          </p>
        </>
      )}

      {mode !== 'omnigent_defaults' && (
        <>
          <label
            className="mt-3 block font-medium text-foreground"
            htmlFor="assistant-model"
          >
            Model{' '}
            <span className="font-normal text-muted-foreground">(optional)</span>
          </label>
          <Input
            id="assistant-model"
            value={model}
            onChange={(e) => setModel(e.target.value)}
            placeholder="model override"
            className="mt-1 h-8"
          />
        </>
      )}

      <div className="mt-3 flex items-center gap-2">
        <Button size="sm" onClick={save} disabled={!valid || update.isPending}>
          {update.isPending && (
            <Loader2 className="animate-spin" aria-hidden="true" />
          )}
          {initial ? 'Save' : 'Save and start chatting'}
        </Button>
        {onDone && (
          <Button
            size="sm"
            variant="ghost"
            onClick={onDone}
            disabled={update.isPending}
          >
            Cancel
          </Button>
        )}
      </div>
      {update.isError && (
        <p className="mt-1.5 text-error">
          Could not save the configuration: {(update.error as Error).message}
        </p>
      )}
    </div>
  )
}
