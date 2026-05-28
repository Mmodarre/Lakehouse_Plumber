"""Import detection for PySpark expressions used in operational metadata.

Provides AST-based detection (with regex fallback) of the imports required to
make a given PySpark expression compile. Used by :class:`OperationalMetadata`
to attach the right ``from pyspark...`` imports to generated code.
"""

import ast
import re
import logging
from typing import Set


class ImportDetector:
    """Detects required imports from PySpark expressions using AST parsing."""

    def __init__(self, strategy: str = "ast"):
        self.logger = logging.getLogger(__name__)
        self.strategy = strategy

        # Fallback regex patterns for when AST parsing fails
        self.fallback_patterns = {
            r"\bF\.": "from pyspark.sql import functions as F",
            r"\budf\(": "from pyspark.sql.functions import udf",
            r"\bpandas_udf\(": "from pyspark.sql.functions import pandas_udf",
            r"\bbroadcast\(": "from pyspark.sql.functions import broadcast",
            r"\bStringType\(\)": "from pyspark.sql.types import StringType",
            r"\bIntegerType\(\)": "from pyspark.sql.types import IntegerType",
            r"\bDoubleType\(\)": "from pyspark.sql.types import DoubleType",
            r"\bBooleanType\(\)": "from pyspark.sql.types import BooleanType",
            r"\bTimestampType\(\)": "from pyspark.sql.types import TimestampType",
        }

        # Function to import mapping for AST parsing
        self.function_imports = {
            ("F", "*"): "from pyspark.sql import functions as F",
            ("udf", None): "from pyspark.sql.functions import udf",
            ("pandas_udf", None): "from pyspark.sql.functions import pandas_udf",
            ("broadcast", None): "from pyspark.sql.functions import broadcast",
            ("StringType", None): "from pyspark.sql.types import StringType",
            ("IntegerType", None): "from pyspark.sql.types import IntegerType",
            ("DoubleType", None): "from pyspark.sql.types import DoubleType",
            ("BooleanType", None): "from pyspark.sql.types import BooleanType",
            ("TimestampType", None): "from pyspark.sql.types import TimestampType",
        }

    def detect_imports(self, expression: str) -> Set[str]:
        """Detect required imports from a PySpark expression.

        Args:
            expression: PySpark expression string

        Returns:
            Set of import statements required
        """
        if self.strategy == "ast":
            return self._detect_imports_ast(expression)
        else:
            return self._detect_imports_regex(expression)

    def _detect_imports_ast(self, expression: str) -> Set[str]:
        """Detect imports using AST parsing with regex fallback."""
        try:
            # Try to parse as an expression
            tree = ast.parse(expression, mode="eval")
            visitor = FunctionCallVisitor()
            visitor.visit(tree)

            imports = set()
            for func_call in visitor.function_calls:
                if len(func_call) == 2:
                    module, function = func_call
                    if function is None:
                        # Direct function call like udf(), StringType()
                        if (module, None) in self.function_imports:
                            imports.add(self.function_imports[(module, None)])
                    else:
                        # Attribute access like F.current_timestamp
                        if (module, "*") in self.function_imports:
                            imports.add(self.function_imports[(module, "*")])

            return imports

        except (SyntaxError, ValueError) as e:
            # Fallback to regex detection
            self.logger.debug(
                f"AST parsing failed for expression '{expression}': {e}. Using regex fallback."
            )
            return self._detect_imports_regex(expression)

    def _detect_imports_regex(self, expression: str) -> Set[str]:
        """Detect imports using regex patterns."""
        imports = set()

        for pattern, import_statement in self.fallback_patterns.items():
            if re.search(pattern, expression):
                imports.add(import_statement)

        return imports


class FunctionCallVisitor(ast.NodeVisitor):
    """AST visitor to collect function calls."""

    def __init__(self):
        self.function_calls = []

    def visit_Call(self, node):
        """Visit function calls like udf(), StringType(), F.current_timestamp()."""
        if isinstance(node.func, ast.Name):
            # Direct function call: udf(), StringType(), etc.
            self.function_calls.append((node.func.id, None))
        elif isinstance(node.func, ast.Attribute):
            # Method call: F.current_timestamp(), obj.method(), etc.
            if isinstance(node.func.value, ast.Name):
                self.function_calls.append((node.func.value.id, node.func.attr))

        self.generic_visit(node)

    def visit_Attribute(self, node):
        """Visit attribute access (e.g., F.current_timestamp)."""
        if isinstance(node.value, ast.Name):
            # This is a simple attribute access like F.current_timestamp
            self.function_calls.append((node.value.id, node.attr))

        self.generic_visit(node)

    def visit_Name(self, node):
        """Visit standalone function names."""
        if isinstance(node.ctx, ast.Load):
            # This is a function name being loaded
            self.function_calls.append((node.id, None))

        self.generic_visit(node)
