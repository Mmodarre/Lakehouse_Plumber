"""``EnhancedSubstitutionManager.substitute_env_tokens`` — the ``${token}``-only pass.

Operational-metadata expressions (issue #289) need environment tokens resolved
WITHOUT the deprecated bare ``{token}`` pass and WITHOUT the secret pass:
expressions are PySpark code, so ``'\\d{8}'`` regex quantifiers and literal
``{word}`` text must survive untouched, and secrets are rejected separately.
"""

from pathlib import Path

import pytest

from lhp.core.processing.substitution import EnhancedSubstitutionManager


def _mgr(tmp_path: Path, body: str, env: str = "dev") -> EnhancedSubstitutionManager:
    sub_file = tmp_path / f"{env}.yaml"
    sub_file.write_text(body)
    return EnhancedSubstitutionManager(substitution_file=sub_file, env=env)


@pytest.mark.unit
class TestSubstituteEnvTokens:
    def test_replaces_dollar_token_with_env_value(self, tmp_path):
        mgr = _mgr(tmp_path, "dev:\n  fd_source: STXX\n")
        assert mgr.substitute_env_tokens("F.lit('${fd_source}')") == "F.lit('STXX')"

    def test_global_tokens_and_reserved_tokens_resolve(self, tmp_path):
        mgr = _mgr(tmp_path, "global:\n  region: emea\ndev:\n  other: x\n")
        assert mgr.substitute_env_tokens("${region}-${logical_env}") == "emea-dev"

    def test_unknown_token_is_left_in_place(self, tmp_path):
        mgr = _mgr(tmp_path, "dev:\n  known: k\n")
        assert mgr.substitute_env_tokens("${known}/${missing}") == "k/${missing}"

    def test_secret_reference_is_left_untouched(self, tmp_path):
        mgr = _mgr(tmp_path, "dev:\n  known: k\n")
        text = "F.lit('${secret:scope/key}')"
        assert mgr.substitute_env_tokens(text) == text
        assert mgr.secret_references == set()

    def test_bare_brace_token_is_not_replaced_even_when_key_exists(self, tmp_path):
        mgr = _mgr(tmp_path, "dev:\n  word: REPLACED\n  '8': EIGHT\n")
        text = "F.regexp_extract(F.col('f'), '{word}_\\\\d{8}', 1)"
        assert mgr.substitute_env_tokens(text) == text
        # The deprecated bare-token flag is only flipped by the bare pass.
        assert mgr.has_deprecated_bare_tokens is False

    def test_text_without_tokens_is_returned_unchanged(self, tmp_path):
        mgr = _mgr(tmp_path, "dev:\n  known: k\n")
        text = 'F.col("a").rlike("\\\\d{8}")'
        assert mgr.substitute_env_tokens(text) == text

    def test_full_yaml_substitution_still_applies_bare_tokens(self, tmp_path):
        """Regression guard: the flowgroup path keeps its legacy bare pass."""
        mgr = _mgr(tmp_path, "dev:\n  catalog: main\n")
        assert mgr.substitute_yaml({"a": "${catalog}.{catalog}"}) == {"a": "main.main"}
