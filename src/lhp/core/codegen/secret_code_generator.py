"""Convert ``__SECRET_scope_key__`` placeholders to valid Python f-strings or bare dbutils calls."""

import logging
import re
from typing import Dict, List, Set, Tuple

from ..processing.substitution import SecretReference

logger = logging.getLogger(__name__)


class SecretCodeGenerator:
    def __init__(self):
        # Regex to find secret placeholders in the format __SECRET_scope_key__
        self.secret_placeholder_pattern = re.compile(r"__SECRET_([^_]+)_([^_]+)__")

        # More robust regex to find string literals (handles escaped quotes)
        # Matches both single and double quoted strings, including those with escaped quotes
        self.string_pattern = re.compile(r'(["\'])((?:\\.|(?!\1)[^\\])*?)\1')

    def generate_python_code(self, code: str, secret_refs: Set[SecretReference]) -> str:
        if not code or not secret_refs:
            return code

        placeholder_to_secret = self._build_placeholder_mapping(secret_refs)
        return self.string_pattern.sub(
            lambda match: self._process_string_literal(match, placeholder_to_secret),
            code,
        )

    def _build_placeholder_mapping(
        self, secret_refs: Set[SecretReference]
    ) -> Dict[str, SecretReference]:
        mapping = {}
        for secret_ref in secret_refs:
            placeholder = f"__SECRET_{secret_ref.scope}_{secret_ref.key}__"
            mapping[placeholder] = secret_ref
        return mapping

    def _process_string_literal(
        self, match, placeholder_to_secret: Dict[str, SecretReference]
    ) -> str:
        quote_char = match.group(1)
        string_content = match.group(2)

        placeholders_in_string = []
        for placeholder, secret_ref in placeholder_to_secret.items():
            if placeholder in string_content:
                placeholders_in_string.append((placeholder, secret_ref))

        if not placeholders_in_string:
            return match.group(0)

        if len(placeholders_in_string) == 1:
            placeholder, secret_ref = placeholders_in_string[0]
            # Only treat as "entire string" if the content exactly matches the placeholder (no whitespace around it)
            if string_content == placeholder:
                dbutils_quote = self._choose_quote_for_dbutils(
                    quote_char, string_content
                )
                return self._generate_dbutils_call(secret_ref, dbutils_quote)

        # Convert to f-string with embedded secrets
        return self._convert_to_fstring(
            string_content, placeholders_in_string, quote_char
        )

    def _choose_quote_for_dbutils(self, outer_quote: str, string_content: str) -> str:
        """Quote selection priority: avoid conflicts, prefer opposite of outer, fall back to single."""
        quote_analysis = self._analyze_quote_usage(string_content)

        if outer_quote == '"':
            if quote_analysis["prefer_double_for_dbutils"]:
                return '"'
            return "'"
        if quote_analysis["prefer_single_for_dbutils"]:
            return "'"
        return '"'

    def _analyze_quote_usage(self, string_content: str) -> Dict[str, bool]:
        regular_single = string_content.count("'")
        regular_double = string_content.count('"')
        escaped_single = string_content.count("\\'")
        escaped_double = string_content.count('\\"')

        single_quote_conflicts = regular_single + escaped_single
        double_quote_conflicts = regular_double + escaped_double

        result = {
            "single_quote_conflicts": single_quote_conflicts,
            "double_quote_conflicts": double_quote_conflicts,
            "prefer_double_for_dbutils": False,
            "prefer_single_for_dbutils": False,
        }

        # Minimum conflict-count difference to override the default preference.
        conflict_threshold = 2

        if single_quote_conflicts >= double_quote_conflicts + conflict_threshold:
            result["prefer_double_for_dbutils"] = True
        elif double_quote_conflicts >= single_quote_conflicts + conflict_threshold:
            result["prefer_single_for_dbutils"] = True

        if self._has_complex_quote_patterns(string_content):
            if single_quote_conflicts > double_quote_conflicts * 2:
                result["prefer_double_for_dbutils"] = True
            else:
                result["prefer_single_for_dbutils"] = True

        return result

    def _has_complex_quote_patterns(self, string_content: str) -> bool:
        complex_patterns = [
            r'\\["\']',  # Escaped quotes
            r'["\'][^"\']*["\']',  # Nested quotes
            r"\\\\",  # Escaped backslashes
            r"\\[ntr]",  # Common escape sequences
        ]

        for pattern in complex_patterns:
            if re.search(pattern, string_content):
                return True

        return False

    def _generate_dbutils_call(
        self, secret_ref: SecretReference, quote_char: str
    ) -> str:
        if quote_char == '"':
            return f'dbutils.secrets.get(scope="{secret_ref.scope}", key="{secret_ref.key}")'
        return (
            f"dbutils.secrets.get(scope='{secret_ref.scope}', key='{secret_ref.key}')"
        )

    def _convert_to_fstring(
        self,
        string_content: str,
        placeholders: List[Tuple[str, SecretReference]],
        quote_char: str,
    ) -> str:
        # Consistent quote choice for all secrets in this string.
        dbutils_quote = self._choose_quote_for_dbutils(quote_char, string_content)

        all_replacements = []
        for placeholder, secret_ref in placeholders:
            # Find all positions of this placeholder in the string
            pos = 0
            while True:
                pos = string_content.find(placeholder, pos)
                if pos == -1:
                    break
                all_replacements.append((pos, placeholder, secret_ref))
                pos += len(placeholder)

        # Replace end-to-start so earlier positions are not shifted.
        all_replacements.sort(key=lambda x: x[0], reverse=True)

        result_content = string_content
        for pos, placeholder, secret_ref in all_replacements:
            dbutils_call = self._generate_dbutils_call(secret_ref, dbutils_quote)
            f_string_expression = f"{{{dbutils_call}}}"

            before = result_content[:pos]
            after = result_content[pos + len(placeholder) :]
            result_content = before + f_string_expression + after

        # Return as f-string
        return f"f{quote_char}{result_content}{quote_char}"
