"""Secret code generator for LakehousePlumber - converts secret placeholders to valid Python f-strings."""

import re
from typing import Set, Dict, List, Tuple
from .substitution import SecretReference


class SecretCodeGenerator:
    """Generate valid Python code with secrets using f-strings and dbutils calls."""

    def __init__(self):
        """Initialize the secret code generator."""
        # Regex to find secret placeholders in the format __SECRET_scope_key__
        self.secret_placeholder_pattern = re.compile(r"__SECRET_([^_]+)_([^_]+)__")

        # More robust regex to find string literals (handles escaped quotes)
        # Matches both single and double quoted strings, including those with escaped quotes
        self.string_pattern = re.compile(r'(["\'])((?:\\.|(?!\1)[^\\])*?)\1')

    def generate_python_code(self, code: str, secret_refs: Set[SecretReference]) -> str:
        """Convert secret placeholders to valid f-string Python code.

        Args:
            code: The input code containing secret placeholders
            secret_refs: Set of SecretReference objects with scope and key information

        Returns:
            Valid Python code with f-strings for secrets
        """
        if not code or not secret_refs:
            return code

        # Create mapping from placeholders to SecretReference objects
        placeholder_to_secret = self._build_placeholder_mapping(secret_refs)

        # Process all string literals in the code
        result = self.string_pattern.sub(
            lambda match: self._process_string_literal(match, placeholder_to_secret),
            code,
        )

        return result

    def _build_placeholder_mapping(
        self, secret_refs: Set[SecretReference]
    ) -> Dict[str, SecretReference]:
        """Build mapping from placeholder patterns to SecretReference objects.

        Args:
            secret_refs: Set of SecretReference objects

        Returns:
            Dictionary mapping placeholder strings to SecretReference objects
        """
        mapping = {}
        for secret_ref in secret_refs:
            placeholder = f"__SECRET_{secret_ref.scope}_{secret_ref.key}__"
            mapping[placeholder] = secret_ref
        return mapping

    def _process_string_literal(
        self, match, placeholder_to_secret: Dict[str, SecretReference]
    ) -> str:
        """Process a single string literal, replacing secret placeholders with f-string expressions.

        Args:
            match: Regex match object for the string literal
            placeholder_to_secret: Mapping from placeholders to SecretReference objects

        Returns:
            Processed string literal with valid Python syntax
        """
        quote_char = match.group(1)  # Either " or '
        string_content = match.group(2)  # Content without surrounding quotes

        # Find all secret placeholders in this string
        placeholders_in_string = []
        for placeholder, secret_ref in placeholder_to_secret.items():
            if placeholder in string_content:
                placeholders_in_string.append((placeholder, secret_ref))

        # If no secrets in this string, return unchanged
        if not placeholders_in_string:
            return match.group(0)  # Return full original match

        # If the entire string content is just one secret (possibly with whitespace), return direct dbutils call
        if len(placeholders_in_string) == 1:
            placeholder, secret_ref = placeholders_in_string[0]
            # Only treat as "entire string" if the content exactly matches the placeholder (no whitespace around it)
            if string_content == placeholder:
                # Entire string is just the secret - return direct dbutils call
                dbutils_quote = self._choose_quote_for_dbutils(
                    quote_char, string_content
                )
                return self._generate_dbutils_call(secret_ref, dbutils_quote)

        # Convert to f-string with embedded secrets
        return self._convert_to_fstring(
            string_content, placeholders_in_string, quote_char
        )

    def _choose_quote_for_dbutils(self, outer_quote: str, string_content: str) -> str:
        """Choose appropriate quote character for dbutils calls based on context.

        Implements intelligent quote selection to avoid conflicts with string content.
        Algorithm priorities:
        1. Avoid conflicts with quotes in the string content
        2. Prefer opposite of outer quote type
        3. Fall back to single quotes when ambiguous

        Args:
            outer_quote: Quote character used for the outer string (" or ')
            string_content: Content of the string to analyze for quote conflicts

        Returns:
            Quote character to use for dbutils calls (" or ')
        """
        # Analyze quote usage in the string content
        quote_analysis = self._analyze_quote_usage(string_content)

        # If outer string uses double quotes
        if outer_quote == '"':
            # Default preference: single quotes for dbutils calls
            # Override if string content has conflicts
            if quote_analysis["prefer_double_for_dbutils"]:
                return '"'
            else:
                return "'"
        else:
            # Outer string uses single quotes
            # Default preference: double quotes for dbutils calls
            # Override if string content has conflicts
            if quote_analysis["prefer_single_for_dbutils"]:
                return "'"
            else:
                return '"'

    def _analyze_quote_usage(self, string_content: str) -> Dict[str, bool]:
        """Analyze quote usage in string content to determine optimal quote choice.

        Args:
            string_content: String content to analyze

        Returns:
            Dictionary with analysis results
        """
        # Count different types of quotes
        regular_single = string_content.count("'")
        regular_double = string_content.count('"')
        escaped_single = string_content.count("\\'")
        escaped_double = string_content.count('\\"')

        # Calculate conflict scores
        single_quote_conflicts = regular_single + escaped_single
        double_quote_conflicts = regular_double + escaped_double

        # Determine preferences
        result = {
            "single_quote_conflicts": single_quote_conflicts,
            "double_quote_conflicts": double_quote_conflicts,
            "prefer_double_for_dbutils": False,
            "prefer_single_for_dbutils": False,
        }

        # If there's a significant difference in quote usage, prefer the less used type
        conflict_threshold = 2  # Minimum difference to override default behavior

        if single_quote_conflicts >= double_quote_conflicts + conflict_threshold:
            # String has significantly more single quotes - prefer double for dbutils
            result["prefer_double_for_dbutils"] = True
        elif double_quote_conflicts >= single_quote_conflicts + conflict_threshold:
            # String has significantly more double quotes - prefer single for dbutils
            result["prefer_single_for_dbutils"] = True

        # Special cases
        if self._has_complex_quote_patterns(string_content):
            # For complex patterns, be more conservative and prefer single quotes
            # unless there are many more single quotes than double quotes
            if single_quote_conflicts > double_quote_conflicts * 2:
                result["prefer_double_for_dbutils"] = True
            else:
                result["prefer_single_for_dbutils"] = True

        return result

    def _has_complex_quote_patterns(self, string_content: str) -> bool:
        """Check if string has complex quote patterns that need special handling.

        Args:
            string_content: String content to check

        Returns:
            True if string has complex quote patterns
        """
        # Look for patterns that indicate complex quoting
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
        """Generate a dbutils.secrets.get() call with specified quote character.

        Args:
            secret_ref: SecretReference object
            quote_char: Quote character to use (" or ')

        Returns:
            dbutils call string with specified quotes
        """
        if quote_char == '"':
            return f'dbutils.secrets.get(scope="{secret_ref.scope}", key="{secret_ref.key}")'
        else:
            return f"dbutils.secrets.get(scope='{secret_ref.scope}', key='{secret_ref.key}')"

    def _convert_to_fstring(
        self,
        string_content: str,
        placeholders: List[Tuple[str, SecretReference]],
        quote_char: str,
    ) -> str:
        """Convert string with placeholders to f-string format.

        Handles multiple secrets in one string with proper quote consistency.

        Args:
            string_content: Original string content (without quotes)
            placeholders: List of (placeholder, SecretReference) tuples
            quote_char: Quote character of the original string

        Returns:
            f-string with dbutils calls
        """
        # Choose quote character for dbutils calls - consistent for all secrets in this string
        dbutils_quote = self._choose_quote_for_dbutils(quote_char, string_content)

        # Find all occurrences of each placeholder (handles multiple instances of same placeholder)
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

        # Sort by position (descending) to replace from end to beginning
        # This prevents position shifts during replacement
        all_replacements.sort(key=lambda x: x[0], reverse=True)

        # Replace each placeholder occurrence with {dbutils.secrets.get(...)}
        result_content = string_content
        for pos, placeholder, secret_ref in all_replacements:
            dbutils_call = self._generate_dbutils_call(secret_ref, dbutils_quote)
            f_string_expression = f"{{{dbutils_call}}}"

            # Replace only at the specific position to handle multiple instances correctly
            before = result_content[:pos]
            after = result_content[pos + len(placeholder) :]
            result_content = before + f_string_expression + after

        # Return as f-string
        return f"f{quote_char}{result_content}{quote_char}"

    def _validate_fstring_result(self, result: str) -> bool:
        """Validate that the generated f-string has valid Python syntax.

        This is a safety check to ensure we're generating valid code.

        Args:
            result: The generated f-string

        Returns:
            True if the syntax is valid, False otherwise
        """
        try:
            # Try to compile the f-string as a Python expression
            compile(result, "<string>", "eval")
            return True
        except SyntaxError:
            return False
