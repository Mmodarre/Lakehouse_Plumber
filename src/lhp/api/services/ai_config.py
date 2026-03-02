"""AI configuration loader with YAML + environment variable overrides.

Loads from ``ai_config.yaml`` in the project root (alongside ``lhp.yaml``),
falling back to sensible defaults if the file is missing.  Environment
variables with the ``LHP_AI_`` prefix override any YAML values.
"""

import logging
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

import yaml

logger = logging.getLogger(__name__)

_DEFAULTS: dict[str, Any] = {
    "provider": "anthropic",
    "model": "anthropic/databricks-claude-sonnet-4-6",
    "allowed_providers": ["anthropic"],
    "allowed_models": {
        "anthropic": [
            "anthropic/databricks-claude-sonnet-4-6",
        ],
    },
    "api_key_env_vars": {
        "anthropic": "ANTHROPIC_API_KEY",
    },
    "provider_config": {
        "anthropic": {
            "models": {
                "databricks-claude-sonnet-4-6": {
                    "name": "Claude Sonnet 4.6 (Databricks)"
                }
            }
        }
    },
    "max_processes": 10,
    "idle_timeout_minutes": 30,
    "session_max_age_hours": 72.0,
}


@dataclass
class AIConfig:
    """Resolved AI configuration (YAML + env overrides merged)."""

    provider: str = _DEFAULTS["provider"]
    model: str = _DEFAULTS["model"]
    allowed_providers: list[str] = field(
        default_factory=lambda: list(_DEFAULTS["allowed_providers"])
    )
    allowed_models: dict[str, list[str]] = field(
        default_factory=lambda: {
            k: list(v) for k, v in _DEFAULTS["allowed_models"].items()
        }
    )
    api_key_env_vars: dict[str, str] = field(
        default_factory=lambda: dict(_DEFAULTS["api_key_env_vars"])
    )
    provider_config: dict[str, Any] = field(
        default_factory=lambda: {
            k: {
                "models": dict(v.get("models", {}))
            }
            for k, v in _DEFAULTS["provider_config"].items()
        }
    )
    max_processes: int = _DEFAULTS["max_processes"]
    idle_timeout_minutes: float = _DEFAULTS["idle_timeout_minutes"]
    session_max_age_hours: float = _DEFAULTS["session_max_age_hours"]

    # ── Factory ───────────────────────────────────────────────

    @classmethod
    def load(cls, project_root: Optional[Path] = None) -> "AIConfig":
        """Load config from YAML then apply ``LHP_AI_*`` env overrides."""
        raw: dict[str, Any] = dict(_DEFAULTS)

        if project_root:
            yaml_path = project_root / "ai_config.yaml"
            if yaml_path.is_file():
                try:
                    with open(yaml_path) as f:
                        data = yaml.safe_load(f)
                    if isinstance(data, dict):
                        raw.update(data)
                        logger.info(f"Loaded AI config from {yaml_path}")
                except Exception:
                    logger.warning(
                        f"Failed to parse {yaml_path}, using defaults",
                        exc_info=True,
                    )

        # Env var overrides (prefix LHP_AI_)
        env_map: dict[str, tuple[str, type]] = {
            "LHP_AI_PROVIDER": ("provider", str),
            "LHP_AI_MODEL": ("model", str),
            "LHP_AI_MAX_PROCESSES": ("max_processes", int),
            "LHP_AI_IDLE_TIMEOUT_MINUTES": ("idle_timeout_minutes", float),
            "LHP_AI_SESSION_MAX_AGE_HOURS": ("session_max_age_hours", float),
        }
        for env_var, (key, cast) in env_map.items():
            val = os.environ.get(env_var)
            if val is not None:
                try:
                    raw[key] = cast(val)
                    logger.debug(f"AI config override: {env_var}={val}")
                except (ValueError, TypeError):
                    logger.warning(f"Invalid value for {env_var}: {val!r}")

        return cls(
            provider=raw["provider"],
            model=raw["model"],
            allowed_providers=raw.get("allowed_providers", _DEFAULTS["allowed_providers"]),
            allowed_models=raw.get("allowed_models", _DEFAULTS["allowed_models"]),
            api_key_env_vars=raw.get("api_key_env_vars", _DEFAULTS["api_key_env_vars"]),
            provider_config=raw.get("provider_config", _DEFAULTS["provider_config"]),
            max_processes=raw["max_processes"],
            idle_timeout_minutes=raw["idle_timeout_minutes"],
            session_max_age_hours=raw.get(
                "session_max_age_hours",
                _DEFAULTS["session_max_age_hours"],
            ),
        )

    # ── Public helpers ────────────────────────────────────────

    def to_opencode_json(
        self,
        provider: Optional[str] = None,
        model: Optional[str] = None,
    ) -> dict[str, Any]:
        """Generate the content for a per-workspace ``opencode.json`` file.

        Produces config with model definitions and, when targeting
        Databricks Model Serving, injects ``options`` for Bearer auth.

        OpenCode v1.2.x bundles ``@ai-sdk/anthropic@2.x`` which only
        supports ``x-api-key`` auth — Databricks rejects that.  The
        workaround is to inject an ``Authorization: Bearer`` header via
        the provider ``options.headers`` block.  Credentials reference
        env vars via the ``{env:VAR}`` syntax and are never inlined.
        """
        provider = provider or self.provider
        model = model or self.model

        # Build provider block with models map from provider_config
        provider_block: dict[str, Any] = {}
        pc = self.provider_config.get(provider, {})
        if pc.get("models"):
            provider_block["models"] = pc["models"]

        # Databricks Model Serving requires Bearer auth.  Inject options
        # so the Vercel AI SDK sends Authorization header instead of x-api-key.
        base_url = os.environ.get("ANTHROPIC_BASE_URL", "")
        if not base_url and os.environ.get("DATABRICKS_HOST"):
            dbx_host = os.environ["DATABRICKS_HOST"].rstrip("/")
            if not dbx_host.startswith(("https://", "http://")):
                dbx_host = f"https://{dbx_host}"
            base_url = f"{dbx_host}/serving-endpoints/anthropic"
        if "databricks" in base_url:
            # Determine the env var name holding the Bearer token.
            # In Databricks Apps (DATABRICKS_CLIENT_ID present),
            # _build_process_env() injects ANTHROPIC_AUTH_TOKEN into
            # the subprocess env even though it's not in the main
            # process env — so always prefer it in that context.
            token_var = "ANTHROPIC_AUTH_TOKEN"
            if (
                not os.environ.get(token_var)
                and "DATABRICKS_CLIENT_ID" not in os.environ
            ):
                token_var = "ANTHROPIC_API_KEY"

            # baseURL must end with /v1 for the SDK to construct
            # the correct path: ${baseURL}/messages
            if not base_url.endswith("/v1"):
                base_url = base_url.rstrip("/") + "/v1"

            provider_block["options"] = {
                "baseURL": base_url,
                "apiKey": "unused",
                "headers": {
                    "Authorization": f"Bearer {{env:{token_var}}}",
                },
            }

        return {
            "$schema": "https://opencode.ai/config.json",
            "provider": {provider: provider_block},
            "model": model,
        }

    def get_allowed_models(self) -> dict[str, list[str]]:
        """Return provider→models mapping for the frontend settings modal."""
        return {
            p: self.allowed_models.get(p, [])
            for p in self.allowed_providers
        }

    def validate_selection(self, provider: str, model: str) -> bool:
        """Check whether a provider/model pair is in the allowed list."""
        if provider not in self.allowed_providers:
            return False
        return model in self.allowed_models.get(provider, [])
