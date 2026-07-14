import { useState } from 'react'
import { ChevronDown, ChevronRight, Loader2, Settings2, X } from 'lucide-react'
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
  usePermissionsConfig,
  usePricingConfig,
  useUpdateExecutorConfig,
  useUpdatePermissionsConfig,
  useUpdatePricingConfig,
} from '../../hooks/useAssistant'
import type {
  ExecutorConfig,
  ExecutorMode,
  ExecutorProvider,
  PermissionRule,
  PricingConfig,
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

// One editable pricing row; rates are kept as raw input strings so partial
// entries ("0.", "") never fight the user, and only parse on save.
interface PricingRow {
  model: string
  input: string
  output: string
  cacheRead: string
  cacheWrite: string
}

function rowsFromConfig(config: PricingConfig): PricingRow[] {
  // `models` is optional in the generated wire type (server default {}).
  return Object.entries(config.models ?? {}).map(([model, rates]) => ({
    model,
    input: String(rates.input_per_mtok),
    output: String(rates.output_per_mtok),
    cacheRead: rates.cache_read_per_mtok != null ? String(rates.cache_read_per_mtok) : '',
    cacheWrite:
      rates.cache_write_per_mtok != null ? String(rates.cache_write_per_mtok) : '',
  }))
}

const EMPTY_ROW: PricingRow = {
  model: '',
  input: '',
  output: '',
  cacheRead: '',
  cacheWrite: '',
}

function parseRate(raw: string): number | null {
  if (raw.trim() === '') return null
  const n = Number(raw)
  return Number.isFinite(n) && n >= 0 ? n : null
}

function configFromRows(rows: PricingRow[]): PricingConfig | null {
  const models: PricingConfig['models'] = {}
  for (const row of rows) {
    if (row.model.trim() === '') continue
    const input = parseRate(row.input)
    const output = parseRate(row.output)
    if (input === null || output === null) return null
    if (row.cacheRead.trim() !== '' && parseRate(row.cacheRead) === null) return null
    if (row.cacheWrite.trim() !== '' && parseRate(row.cacheWrite) === null) return null
    models[row.model.trim()] = {
      input_per_mtok: input,
      output_per_mtok: output,
      cache_read_per_mtok: parseRate(row.cacheRead),
      cache_write_per_mtok: parseRate(row.cacheWrite),
    }
  }
  return { models }
}

/**
 * Collapsible per-project model pricing (used to compute costs when the SDK
 * estimate is meaningless — Databricks auth). Keys may be exact model ids or
 * id-prefixes; omitted cache rates default server-side (0.1x / 1.25x input).
 * Saving PUTs `/assistant/pricing`, which never touches session staleness.
 */
function PricingSection() {
  const [open, setOpen] = useState(false)
  const pricing = usePricingConfig({ enabled: open })
  const update = useUpdatePricingConfig()
  // null = untouched: rows derive from the stored config until the first
  // edit, then the local copy owns the form (no state-sync effect).
  const [edits, setEdits] = useState<PricingRow[] | null>(null)

  const storedRows =
    pricing.data === undefined ? null : rowsFromConfig(pricing.data)
  const rows =
    edits ??
    (storedRows === null
      ? null
      : storedRows.length > 0
        ? storedRows
        : [EMPTY_ROW])

  const setRow = (index: number, patch: Partial<PricingRow>) =>
    setEdits(
      (rows ?? []).map((r, i) => (i === index ? { ...r, ...patch } : r)),
    )

  const body = configFromRows(rows ?? [])

  return (
    <div className="mt-3">
      <button
        type="button"
        onClick={() => setOpen((v) => !v)}
        className="flex items-center gap-1 font-medium text-foreground"
        aria-expanded={open}
      >
        {open ? (
          <ChevronDown className="size-3" aria-hidden="true" />
        ) : (
          <ChevronRight className="size-3" aria-hidden="true" />
        )}
        Model pricing{' '}
        <span className="font-normal text-muted-foreground">(optional)</span>
      </button>
      {open && rows !== null && (
        <div className="mt-1.5">
          <p>
            USD per million tokens. Model can be an exact id or a prefix
            (e.g. `claude-sonnet-`). Used to show costs when signing in with
            Databricks.
          </p>
          {rows.map((row, i) => (
            <div key={i} className="mt-1.5 flex items-start gap-1">
              <div className="grid flex-1 grid-cols-2 gap-1">
                <Input
                  value={row.model}
                  onChange={(e) => setRow(i, { model: e.target.value })}
                  placeholder="model id or prefix"
                  aria-label={`Pricing model ${i + 1}`}
                  className="col-span-2 h-7 text-xs"
                />
                <Input
                  value={row.input}
                  onChange={(e) => setRow(i, { input: e.target.value })}
                  placeholder="input $/MTok"
                  aria-label={`Input rate ${i + 1}`}
                  className="h-7 text-xs"
                />
                <Input
                  value={row.output}
                  onChange={(e) => setRow(i, { output: e.target.value })}
                  placeholder="output $/MTok"
                  aria-label={`Output rate ${i + 1}`}
                  className="h-7 text-xs"
                />
                <Input
                  value={row.cacheRead}
                  onChange={(e) => setRow(i, { cacheRead: e.target.value })}
                  placeholder="cache read: 0.1 × input"
                  aria-label={`Cache read rate ${i + 1}`}
                  className="h-7 text-xs"
                />
                <Input
                  value={row.cacheWrite}
                  onChange={(e) => setRow(i, { cacheWrite: e.target.value })}
                  placeholder="cache write: 1.25 × input"
                  aria-label={`Cache write rate ${i + 1}`}
                  className="h-7 text-xs"
                />
              </div>
              <Button
                variant="ghost"
                size="icon-sm"
                onClick={() => setEdits((rows ?? []).filter((_, j) => j !== i))}
                aria-label={`Remove pricing row ${i + 1}`}
                className="text-muted-foreground"
              >
                <X aria-hidden="true" />
              </Button>
            </div>
          ))}
          <div className="mt-1.5 flex items-center gap-2">
            <Button
              size="sm"
              variant="outline"
              onClick={() => setEdits([...(rows ?? []), EMPTY_ROW])}
            >
              Add model
            </Button>
            <Button
              size="sm"
              onClick={() => body !== null && update.mutate(body)}
              disabled={body === null || update.isPending}
            >
              {update.isPending && (
                <Loader2 className="animate-spin" aria-hidden="true" />
              )}
              Save pricing
            </Button>
          </div>
          {update.isError && (
            <p className="mt-1.5 text-error">
              Could not save pricing: {(update.error as Error).message}
            </p>
          )}
          {update.isSuccess && !update.isPending && (
            <p className="mt-1.5">Pricing saved.</p>
          )}
        </div>
      )}
    </div>
  )
}

function ruleLabel(rule: PermissionRule): string {
  return rule.prefix != null && rule.prefix !== ''
    ? `${rule.tool}: "${rule.prefix}"`
    : rule.tool
}

/**
 * Collapsible list of the project's "Always allow" rules (created from the
 * approval card's third button). Deleting a rule PUTs the filtered list to
 * `/assistant/permissions`, which never touches session staleness.
 */
function PermissionsSection() {
  const [open, setOpen] = useState(false)
  const permissions = usePermissionsConfig({ enabled: open })
  const update = useUpdatePermissionsConfig()

  const rules = permissions.data?.always_allow ?? null

  const removeRule = (index: number) => {
    if (rules === null) return
    update.mutate({ always_allow: rules.filter((_, i) => i !== index) })
  }

  return (
    <div className="mt-3">
      <button
        type="button"
        onClick={() => setOpen((v) => !v)}
        className="flex items-center gap-1 font-medium text-foreground"
        aria-expanded={open}
      >
        {open ? (
          <ChevronDown className="size-3" aria-hidden="true" />
        ) : (
          <ChevronRight className="size-3" aria-hidden="true" />
        )}
        Always-allow rules
      </button>
      {open && (
        <div className="mt-1.5">
          <p>
            Tools and command prefixes approved with “Always allow” run
            without asking. Remove a rule to make them ask again.
          </p>
          {rules !== null && rules.length === 0 && (
            <p className="mt-1.5 text-muted-foreground">No rules yet.</p>
          )}
          {rules !== null && rules.length > 0 && (
            <ul className="mt-1.5">
              {rules.map((rule, i) => (
                <li
                  key={`${rule.tool}\u0000${rule.prefix ?? ''}`}
                  className="flex items-center justify-between gap-1 py-0.5"
                >
                  <span className="min-w-0 truncate font-mono text-2xs text-foreground">
                    {ruleLabel(rule)}
                  </span>
                  <Button
                    variant="ghost"
                    size="icon-sm"
                    onClick={() => removeRule(i)}
                    disabled={update.isPending}
                    aria-label={`Remove rule ${ruleLabel(rule)}`}
                    className="text-muted-foreground"
                  >
                    <X aria-hidden="true" />
                  </Button>
                </li>
              ))}
            </ul>
          )}
          {update.isError && (
            <p className="mt-1.5 text-error">
              Could not update the rules: {(update.error as Error).message}
            </p>
          )}
        </div>
      )}
    </div>
  )
}

/**
 * Provider + auth choice card. First use: shown until a config is stored,
 * blocking the composer (no `initial`, no `onDone`) — the PRODUCT default
 * `claude_sdk` is preselected. Reconfigure: opened from the panel's gear
 * button with the stored config prefilled; saving (or Cancel) returns to the
 * chat. Saving PUTs the config — the backend marks the active session stale,
 * so the next message reprovisions. Model pricing is a separate PUT that
 * never touches staleness.
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

      <PricingSection />
      <PermissionsSection />

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
