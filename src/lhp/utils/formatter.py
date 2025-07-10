"""Code formatting utilities for LakehousePlumber.

Provides utilities for formatting generated Python code.
Reused from BurrowBuilder.
"""

import logging
import re
from typing import List
import tempfile
import subprocess
from pathlib import Path


class CodeFormatter:
    """Format generated Python code using Black."""

    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def format_code(self, code: str, line_length: int = 100) -> str:
        """Format Python code using Black.

        Args:
            code: Python code to format
            line_length: Maximum line length

        Returns:
            Formatted code
        """
        try:
            # Try to use Black programmatically
            import black

            # Format with Black
            mode = black.Mode(
                line_length=line_length,
                target_versions={black.TargetVersion.PY38},
                string_normalization=True,
                magic_trailing_comma=True,
            )

            formatted = black.format_str(code, mode=mode)
            return formatted

        except ImportError:
            self.logger.warning("Black not available, trying command line")
            return self._format_with_black_cli(code, line_length)
        except Exception as e:
            self.logger.warning(f"Black formatting failed: {e}")
            # Return organized code even if Black fails
            return self.organize_imports(code)

    def _format_with_black_cli(self, code: str, line_length: int = 100) -> str:
        """Format code using Black CLI.

        Args:
            code: Python code to format
            line_length: Maximum line length

        Returns:
            Formatted code
        """
        try:
            # Write code to temporary file
            with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False) as f:
                f.write(code)
                temp_file = f.name

            # Run Black
            result = subprocess.run(
                ["black", "-l", str(line_length), temp_file],
                capture_output=True,
                text=True,
            )

            if result.returncode == 0:
                # Read formatted code
                with open(temp_file, "r") as f:
                    formatted = f.read()
                return formatted
            else:
                self.logger.warning(f"Black CLI failed: {result.stderr}")
                return code

        except Exception as e:
            self.logger.warning(f"Black CLI formatting failed: {e}")
            return code
        finally:
            # Clean up temp file
            try:
                Path(temp_file).unlink()
            except:
                pass

    def organize_imports(self, code: str) -> str:
        """Organize imports in Python code.

        Args:
            code: Python code

        Returns:
            Code with organized imports
        """
        lines = code.split("\n")

        # Separate imports and other code
        import_lines = []
        other_lines = []
        in_imports = True

        for line in lines:
            stripped = line.strip()

            if in_imports:
                if stripped.startswith(("import ", "from ")):
                    import_lines.append(line)
                elif stripped and not stripped.startswith("#"):
                    # First non-import, non-comment line
                    in_imports = False
                    other_lines.append(line)
                else:
                    # Comments and blank lines
                    if import_lines:
                        # Already have imports, this is a separator
                        in_imports = False
                    other_lines.append(line)
            else:
                other_lines.append(line)

        # Sort imports
        import_lines = self._sort_imports(import_lines)

        # Combine
        if import_lines:
            return "\n".join(import_lines) + "\n\n" + "\n".join(other_lines)
        else:
            return "\n".join(other_lines)

    def _sort_imports(self, import_lines: List[str]) -> List[str]:
        """Sort import statements.

        Args:
            import_lines: List of import lines

        Returns:
            Sorted import lines
        """
        # Separate standard library, third-party, and local imports
        stdlib_imports = []
        third_party_imports = []
        local_imports = []

        stdlib_modules = {
            "os",
            "sys",
            "datetime",
            "json",
            "logging",
            "pathlib",
            "re",
            "collections",
            "itertools",
            "functools",
            "typing",
        }

        for line in import_lines:
            if not line.strip():
                continue

            # Extract module name
            if line.strip().startswith("import "):
                module = line.strip().split()[1].split(".")[0]
            elif line.strip().startswith("from "):
                module = line.strip().split()[1].split(".")[0]
            else:
                continue

            if module in stdlib_modules:
                stdlib_imports.append(line)
            elif module.startswith("."):
                local_imports.append(line)
            else:
                third_party_imports.append(line)

        # Sort each group
        stdlib_imports.sort()
        third_party_imports.sort()
        local_imports.sort()

        # Combine with proper spacing
        result = []
        if stdlib_imports:
            result.extend(stdlib_imports)
        if third_party_imports:
            if result:
                result.append("")
            result.extend(third_party_imports)
        if local_imports:
            if result:
                result.append("")
            result.extend(local_imports)

        return result

    def format_sql(self, sql: str, indent: int = 4) -> str:
        """Format SQL query for readability.

        Args:
            sql: SQL query string
            indent: Number of spaces for indentation

        Returns:
            Formatted SQL
        """
        # Basic SQL formatting
        formatted = sql

        # Add newlines before keywords (handle multi-word keywords first)
        multi_keywords = [
            "LEFT JOIN",
            "RIGHT JOIN",
            "INNER JOIN",
            "OUTER JOIN",
            "GROUP BY",
            "ORDER BY",
        ]
        for keyword in multi_keywords:
            pattern = rf"\b{keyword}\b"
            formatted = re.sub(pattern, f"\n{keyword}", formatted, flags=re.IGNORECASE)

        # Then handle single-word keywords
        single_keywords = [
            "FROM",
            "WHERE",
            "JOIN",
            "ON",
            "HAVING",
            "UNION",
            "LIMIT",
            "OFFSET",
            "WITH",
            "AS",
        ]
        for keyword in single_keywords:
            pattern = rf"\b{keyword}\b"
            formatted = re.sub(pattern, f"\n{keyword}", formatted, flags=re.IGNORECASE)

        # Clean up multiple spaces
        formatted = re.sub(r"  +", " ", formatted)

        # Remove space after newline
        formatted = re.sub(r"\n ", "\n", formatted)

        # Indent lines
        lines = formatted.strip().split("\n")
        indented_lines = []

        for i, line in enumerate(lines):
            if i == 0:
                indented_lines.append(line)
            else:
                indented_lines.append(" " * indent + line.strip())

        return "\n".join(indented_lines)


def format_code(code: str, line_length: int = 100) -> str:
    """Convenience function to format code.

    Args:
        code: Python code to format
        line_length: Maximum line length

    Returns:
        Formatted code
    """
    formatter = CodeFormatter()
    return formatter.format_code(code, line_length)


def organize_imports(code: str) -> str:
    """Convenience function to organize imports.

    Args:
        code: Python code

    Returns:
        Code with organized imports
    """
    formatter = CodeFormatter()
    return formatter.organize_imports(code)


def format_sql(sql: str, indent: int = 4) -> str:
    """Convenience function to format SQL.

    Args:
        sql: SQL query string
        indent: Number of spaces for indentation

    Returns:
        Formatted SQL
    """
    formatter = CodeFormatter()
    return formatter.format_sql(sql, indent)
