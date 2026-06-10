"""Secret-reference post-pass for generated pipeline code.

:stability: internal
"""

import logging
from typing import List

from ..processing.substitution import EnhancedSubstitutionManager


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
        complete_code = "\n\n".join(generated_sections)

        try:
            from .secret_code_generator import SecretCodeGenerator

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
