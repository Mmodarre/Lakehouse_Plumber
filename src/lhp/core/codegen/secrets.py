"""Secret-reference post-pass for generated pipeline code.

:stability: internal
"""

import logging
from typing import List

from ...utils.substitution import EnhancedSubstitutionManager


class SecretSubstitutor:
    """Rewrite ``__SECRET_scope_key__`` placeholders into ``dbutils.secrets.get(...)``.

    Secrets are emitted as ``__SECRET_scope_key__`` placeholders during YAML
    substitution so that Jinja templates can wrap values in Python string
    literals without disturbing the secret expression. This post-pass walks
    the assembled code and rewrites placeholders into valid Python
    (bare calls or f-strings).

    :stability: internal
    """

    def __init__(self) -> None:
        self.logger = logging.getLogger(__name__)

    def apply(
        self,
        generated_sections: List[str],
        substitution_mgr: EnhancedSubstitutionManager,
    ) -> str:
        """Apply secret substitutions to generated code.

        Secrets are emitted as ``__SECRET_scope_key__`` placeholders during
        YAML substitution so that Jinja templates can wrap values in Python
        string literals without disturbing the secret expression. This
        post-pass walks the assembled code and rewrites placeholders:

        - When a string literal's content is exactly one placeholder, it is
          replaced with a bare ``dbutils.secrets.get(...)`` call so that
          fields like ``.option("user", "${secret:db/user}")`` become
          ``.option("user", dbutils.secrets.get(...))``.
        - When a placeholder is embedded in a larger string literal, the
          literal is rewritten as an f-string so the dbutils call evaluates
          at runtime.
        """
        complete_code = "\n\n".join(generated_sections)

        # Use SecretCodeGenerator to convert secret placeholders to valid
        # Python (bare calls or f-strings). Pull references from the
        # substitution manager — it is the canonical source.
        try:
            from ...utils.secret_code_generator import SecretCodeGenerator

            secret_generator = SecretCodeGenerator()
            complete_code = secret_generator.generate_python_code(
                complete_code, substitution_mgr.secret_references
            )
        except ImportError:
            self.logger.warning(
                "SecretCodeGenerator not available, skipping secret substitutions"
            )
        except Exception as e:
            self.logger.warning(f"Error applying secret substitutions: {e}")

        return complete_code
