"""Domain exception types for LHP.

Frozen-attribute, picklable error classes following ``LHP-<CAT>-<NNN>``
code-stamped convention. No Rich rendering; visual presentation lives in
``lhp.cli.error_panel.render_error_panel`` (consumed by
``lhp.cli.error_boundary`` and the validate command's per-pipeline
panel loop).
"""

# JUSTIFIED: types.py sits at ~574 lines because it hosts the full
# closed set of LHP domain exception types — the base ``LHPError``
# (with its plain-text formatter and pickle protocol), the three
# dual-inheritance ``ValueError``/``FileNotFoundError`` bridges
# (``LHPValidationError``/``LHPConfigError``/``LHPFileError``), the
# worker-side reconstruction helper, ``MultiDocumentError``, and the
# four reparented bundle exceptions (``BundleResourceError``,
# ``TemplateError``, ``YAMLProcessingError``, ``BundleConfigurationError``).
# Each class is small, but they form one cohesive class hierarchy and
# splitting them across multiple files (`types_bundle.py`,
# `types_worker.py`, ...) would (a) break the constitution §2.2 rule
# that domain types live under ``lhp.errors`` as one logical unit and
# (b) create import-ordering hazards for the ``__reduce__`` /
# ``_WORKER_ERROR_TYPE_TO_LHP_CLASS`` registry that resolves by class
# name across the spawn boundary. The inline ``__init__``
# legacy-attribute preservation is load-bearing for
# ``convert_bundle_error``'s ``getattr`` paths.

from __future__ import annotations

import textwrap
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from .categories import ErrorCategory


class LHPError(Exception):
    """User-friendly error with formatting support.

    Base class for all LHP-specific exceptions. Subclasses use dual
    inheritance (e.g. ``LHPValidationError(LHPError, ValueError)``)
    so that existing ``except ValueError`` handlers still catch them.
    """

    def __init__(
        self,
        category: ErrorCategory,
        code_number: str,
        title: str,
        details: str,
        suggestions: Optional[List[str]] = None,
        example: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None,
        doc_link: Optional[str] = None,
    ):
        self.category = category
        self.code_number = code_number
        self.code = f"LHP-{category.value}-{code_number}"
        self.title = title
        self.details = details
        self.suggestions = suggestions or []
        self.example = example
        self.context = context or {}
        self.doc_link = (
            doc_link
            or "https://lakehouse-plumber.readthedocs.io/en/latest/errors_reference.html"
        )

        # Format the complete error message
        super().__init__(self._format_message())

    def _category_label(self) -> str:
        """Return a human-readable label for the error category."""
        return {
            ErrorCategory.VALIDATION: "Validation Error",
            ErrorCategory.CONFIG: "Configuration Error",
            ErrorCategory.IO: "I/O Error",
            ErrorCategory.DEPENDENCY: "Dependency Error",
            ErrorCategory.ACTION: "Action Error",
            ErrorCategory.CLOUDFILES: "CloudFiles Error",
            ErrorCategory.GENERAL: "Error",
        }.get(self.category, "Error")

    def _format_message(self) -> str:
        """Format the error as a plain-text ASCII message.

        Used by ``__str__`` and the file logger. No emoji, no ANSI escapes —
        safe for log files, piped output, and non-TTY contexts.
        """
        lines = [f"Error [{self.code}]: {self.title}"]
        lines.append("=" * 70)
        if self.details:
            lines.append("")
            lines.append(textwrap.fill(self.details, width=70))
        if self.context:
            lines.append("")
            lines.append("Context")
            for key, value in self.context.items():
                lines.append(f"  {key}: {value}")
        if self.suggestions:
            lines.append("")
            lines.append("Suggestions")
            for suggestion in self.suggestions:
                wrapped = textwrap.fill(suggestion, width=66, subsequent_indent="     ")
                lines.append(f"  -> {wrapped}")
        if self.example:
            lines.append("")
            lines.append("Example")
            for line in self.example.strip().split("\n"):
                lines.append(f"  {line}")
        if self.doc_link:
            lines.append("")
            lines.append(f"More info: {self.doc_link}")
        lines.append("=" * 70)
        return "\n".join(lines)

    def __reduce__(self):
        return (
            self.__class__,
            (
                self.category,
                self.code_number,
                self.title,
                self.details,
                self.suggestions,
                self.example,
                self.context,
                self.doc_link,
            ),
        )

    @classmethod
    def from_unexpected_exception(
        cls,
        exc: BaseException,
        operation: str,
    ) -> "LHPError":
        """Wrap a non-LHP, non-Bundle exception as an LHPError for CLI rendering.

        Used by :func:`cli_error_boundary` to surface generic fallback failures
        through the project's unified Rich panel UX rather than as plain stderr
        lines. The full Python traceback is intentionally NOT carried on the
        returned LHPError; callers are expected to ``logger.exception(...)`` so
        the traceback lands in the log file only.
        """
        return cls(
            category=ErrorCategory.GENERAL,
            code_number="902",
            title=f"{operation} failed: unexpected error",
            details=str(exc) or repr(exc),
            context={"exception_type": type(exc).__name__},
            suggestions=[
                "Re-run with --verbose to keep the traceback in your terminal",
                "Check the log file for the full Python traceback",
                "If this is reproducible, please file an issue with the log excerpt",
            ],
        )


class LHPValidationError(LHPError, ValueError):
    """LHPError subclass that is also a ValueError.

    Use when replacing a bare ValueError so that existing
    ``except ValueError`` handlers continue to catch it.
    """

    pass


class PythonFunctionConflictError(LHPValidationError):
    """Raised when different Python source files would create same destination file."""

    def __init__(self, destination: str, existing_source: str, new_source: str):
        self.destination = destination
        self.existing_source = existing_source
        self.new_source = new_source

        super().__init__(
            category=ErrorCategory.VALIDATION,
            code_number="019",
            title="Python function naming conflict",
            details=(
                f"Two different Python source files would create the same destination file.\n"
                f"  Existing: {existing_source} -> {destination}\n"
                f"  New:      {new_source} -> {destination}"
            ),
            suggestions=[
                "Rename one of the Python functions",
                "Move functions to different directories",
                "Update YAML module_path to use a different name",
            ],
            context={
                "Destination": destination,
                "Existing source": existing_source,
                "New source": new_source,
            },
        )

    def __reduce__(self) -> tuple[Any, ...]:
        # Override the parent ``LHPError.__reduce__`` (which reconstructs
        # with the 8-arg base ``__init__`` signature) so this subclass's
        # custom 3-arg ``__init__(destination, existing_source,
        # new_source)`` survives the spawn-pool pickle round-trip.
        # Without this override, ``PipelineDelta.lhp_error`` cannot ship
        # this subclass across the worker→main boundary and the original
        # LHP-VAL-019 error is silently replaced by an unpickling
        # ``TypeError`` on the parent side.
        return (
            self.__class__,
            (self.destination, self.existing_source, self.new_source),
        )


class LHPConfigError(LHPError, ValueError):
    """LHPError subclass for configuration errors.

    Also a ValueError for backward compatibility with existing
    ``except ValueError`` handlers.
    """

    pass


class LHPFileError(LHPError, FileNotFoundError):
    """LHPError subclass that is also a FileNotFoundError.

    Use when replacing a bare FileNotFoundError so that existing
    ``except FileNotFoundError`` handlers continue to catch it.
    """

    pass


_WORKER_ERROR_TYPE_TO_LHP_CLASS: Dict[str, type] = {
    # Common stdlib exception names map to the LHP subclass that carries
    # the matching dual inheritance, so legacy ``except ValueError`` /
    # ``except FileNotFoundError`` handlers still catch worker failures
    # surfaced across the spawn boundary. LHPError instances raised in
    # workers do NOT round-trip through this mapping: they travel back
    # via ``PipelineDelta.lhp_error`` and are re-raised unchanged.
    "ValueError": LHPValidationError,
    "FileNotFoundError": LHPFileError,
    "OSError": LHPFileError,
    "IOError": LHPFileError,
    "PermissionError": LHPFileError,
    "IsADirectoryError": LHPFileError,
    "NotADirectoryError": LHPFileError,
}


def lhp_error_from_worker_failure(
    *,
    pipeline_name: str,
    error_type: str,
    error_message: str,
    error_traceback: str,
) -> LHPError:
    """Reconstruct a worker-side exception with type fidelity preserved.

    Dispatches to the right :class:`LHPError` subclass based on
    ``error_type`` so that callers catching ``ValueError`` /
    ``FileNotFoundError`` / etc. continue to catch worker failures the
    same way they caught them when generation ran in the main thread.
    Unknown types fall back to plain :class:`LHPError`.

    LHPError instances raised in workers do NOT go through this function:
    they travel back via :attr:`PipelineDelta.lhp_error` and are re-raised
    unchanged by the orchestrator / layers consumer.
    """
    target_cls = _WORKER_ERROR_TYPE_TO_LHP_CLASS.get(error_type, LHPError)
    first_trace_line = (
        error_traceback.strip().splitlines()[-1] if error_traceback else ""
    )
    details = error_message or "(no message)"
    if first_trace_line and first_trace_line not in details:
        details = f"{details}\n[{first_trace_line}]"
    return target_cls(
        category=ErrorCategory.GENERAL,
        code_number="901",
        title=f"Pipeline '{pipeline_name}' failed in worker ({error_type})",
        details=details,
        context={"pipeline": pipeline_name, "worker_exception": error_type},
    )


class MultiDocumentError(LHPError):
    """Error raised when a single-document loader encounters wrong number of documents."""

    def __init__(
        self,
        file_path: Union[Path, str],
        num_documents: int,
        error_context: Optional[str] = None,
    ):
        """
        Initialize MultiDocumentError.

        Args:
            file_path: Path to the YAML file
            num_documents: Number of documents found (0 for empty, 2+ for multi-document)
            error_context: Optional context for error message
        """
        # Normalize to Path for consistent handling
        file_path = Path(file_path)
        self.file_path = file_path
        self.num_documents = num_documents
        self.error_context = error_context
        context_str = error_context or f"YAML file {file_path}"

        if num_documents == 0:
            details = (
                f"The file '{file_path}' is empty or contains no valid YAML documents."
            )
            suggestions = [
                "Ensure the file contains valid YAML content",
                "Check that the file is not empty",
                "Verify the file encoding is UTF-8",
            ]
        else:
            details = f"The {context_str} contains {num_documents} documents (separated by '---'), but expected exactly 1."
            suggestions = [
                "Use load_yaml_documents_all() for multi-document YAML files",
                "Remove extra '---' separators if you intended a single document",
                "Split the file into separate files, one per document",
            ]

        super().__init__(
            category=ErrorCategory.IO,
            code_number="003",
            title=f"Invalid Document Count: Expected 1, Found {num_documents}",
            details=details,
            suggestions=suggestions,
            context={"file_path": str(file_path), "num_documents": num_documents},
        )

    def __reduce__(self):
        return (
            self.__class__,
            (self.file_path, self.num_documents, self.error_context),
        )


class BundleResourceError(LHPError):
    """LHPError subclass for bundle resource operation failures.

    Carries the Databricks Asset Bundle resource error semantics:
    YAML resource file generation, sync failures, directory access
    issues. Renders as ``LHP-CFG-020`` via the standard Rich panel
    pipeline.

    The legacy ``Exception``-based class lived in
    :mod:`lhp.bundle.exceptions`; it is now a re-export shim that
    resolves here. See ``LOCAL/TARGET_ARCHITECTURE.md`` §6.
    """

    _category = ErrorCategory.CONFIG
    _code_number = "020"
    _title_template = "Bundle resource error during {operation}"
    _default_suggestions = (
        "Check your databricks.yml configuration",
        "Verify bundle resource files are valid YAML",
        "Run 'lhp validate' to check configuration",
    )

    def __init__(
        self,
        message: str,
        original_error: Optional[Exception] = None,
        operation: str = "bundle operation",
    ):
        """Initialize a bundle resource error.

        Args:
            message: Human-readable error message; becomes ``details``
            original_error: Underlying exception (legacy attribute,
                preserved so ``convert_bundle_error`` ``getattr`` paths
                keep working until D9 removes them)
            operation: Bundle operation name; threads into the title
                template (e.g. ``"sync"``, ``"parse"``)
        """
        context: Dict[str, Any] = {"Operation": operation}
        if original_error is not None:
            context["Original Error"] = type(original_error).__name__
        super().__init__(
            category=self._category,
            code_number=self._code_number,
            title=self._title_template.format(operation=operation),
            details=message,
            suggestions=list(self._default_suggestions),
            context=context,
            example=None,
            doc_link=None,
        )
        self.original_error = original_error
        self.operation = operation

    def __reduce__(self):
        return (
            self.__class__,
            (self.details, self.original_error, self.operation),
        )


class TemplateError(LHPError):
    """LHPError subclass for bundle template fetch / processing failures.

    Covers GitHub template downloads, template rendering, and template
    application during bundle initialization. Renders as ``LHP-CFG-024``.

    The legacy ``Exception``-based class lived in
    :mod:`lhp.bundle.exceptions`; it is now a re-export shim.
    """

    _category = ErrorCategory.CONFIG
    _code_number = "024"
    _title_template = "Bundle template error during {operation}"
    _default_suggestions = (
        "Check your internet connection if downloading templates",
        "Verify the template path or URL is correct",
        "Try running the command again",
    )

    def __init__(
        self,
        message: str,
        original_error: Optional[Exception] = None,
        operation: str = "template operation",
    ):
        """Initialize a template error.

        Args:
            message: Human-readable error message; becomes ``details``
            original_error: Underlying exception (legacy attribute,
                preserved for ``convert_bundle_error`` compatibility
                until D9 removes the factory)
            operation: Template operation name; threads into title
        """
        context: Dict[str, Any] = {"Operation": operation}
        if original_error is not None:
            context["Original Error"] = type(original_error).__name__
        super().__init__(
            category=self._category,
            code_number=self._code_number,
            title=self._title_template.format(operation=operation),
            details=message,
            suggestions=list(self._default_suggestions),
            context=context,
            example=None,
            doc_link=None,
        )
        self.original_error = original_error
        self.operation = operation

    def __reduce__(self):
        return (
            self.__class__,
            (self.details, self.original_error, self.operation),
        )


class YAMLProcessingError(LHPError):
    """LHPError subclass for bundle YAML parsing / processing failures.

    Used for errors in parsing, validating, or updating bundle resource
    YAML files. Renders as ``LHP-CFG-021``.

    Unlike the legacy class, this no longer subclasses
    :class:`BundleResourceError` — both are independent ``LHPError``
    subclasses with distinct error codes. ``isinstance(e, BundleResourceError)``
    checks that relied on the old chain must be updated; the CLI error
    boundary already handles each type explicitly.
    """

    _category = ErrorCategory.CONFIG
    _code_number = "021"
    _title_template = "Bundle YAML processing error"
    _default_suggestions = (
        "Check YAML syntax (indentation, colons, dashes)",
        "Validate the file with a YAML linter",
        "Ensure the file is not corrupted or truncated",
    )

    def __init__(
        self,
        message: str,
        file_path: Optional[str] = None,
        line_number: Optional[int] = None,
        context: Optional[str] = None,
        original_error: Optional[Exception] = None,
    ):
        """Initialize a YAML processing error.

        Args:
            message: Human-readable error message
            file_path: Path to the YAML file that caused the error
            line_number: 1-based line number where the error occurred
            context: Extra context string (e.g. surrounding YAML section)
            original_error: Underlying exception (preserved for legacy
                ``getattr`` callers in ``convert_bundle_error``)

        The constructed ``details`` field mirrors the legacy formatter
        so existing log lines do not change byte-for-byte.
        """
        # Build the details string to match the legacy concatenation
        # exactly (matters for downstream string-matching tests and for
        # the eventual D9 collapse of convert_bundle_error).
        if file_path:
            full_message = f"YAML processing error in {file_path}: {message}"
        else:
            full_message = f"YAML processing failed: {message}"
        if line_number:
            full_message += f" (line {line_number})"
        if context:
            full_message += f"\nContext: {context}"
        if original_error is not None:
            full_message += f"\nOriginal error: {original_error}"

        error_context: Dict[str, Any] = {}
        if file_path is not None:
            error_context["File"] = file_path
        if line_number is not None:
            error_context["Line"] = str(line_number)

        super().__init__(
            category=self._category,
            code_number=self._code_number,
            title=self._title_template,
            details=full_message,
            suggestions=list(self._default_suggestions),
            context=error_context,
            example=None,
            doc_link=None,
        )
        # Raw constructor input preserved for ``__reduce__``: passing
        # ``self.details`` (the decorated full_message) back through
        # ``__init__`` on unpickle would re-append Context / Original
        # error lines.
        self._raw_message = message
        # Preserve legacy public attributes so existing ``getattr``
        # paths in ``convert_bundle_error`` keep resolving until D9
        # removes the factory.
        self.file_path = file_path
        self.line_number = line_number
        # The legacy free-form context string lives on
        # ``self.yaml_context``; it CANNOT overload ``self.context``
        # without breaking ``render_error_panel`` (which expects a
        # dict). Callers that previously read ``error.context`` (a
        # free-form string in the legacy class) must migrate to
        # ``error.yaml_context``; the factory test was updated
        # accordingly.
        self.yaml_context = context
        self.original_error = original_error

    def __reduce__(self):
        return (
            self.__class__,
            (
                self._raw_message,
                self.file_path,
                self.line_number,
                self.yaml_context,
                self.original_error,
            ),
        )


class BundleConfigurationError(LHPError):
    """LHPError subclass for invalid bundle configuration.

    Used for errors in bundle structure, missing configuration files,
    or invalid bundle settings. Renders as ``LHP-CFG-025``.

    The legacy class subclassed :class:`BundleResourceError`; this
    independent subclass keeps the legacy code number but no longer
    sits below ``BundleResourceError`` in the type hierarchy. Callers
    that need to catch either should use a tuple of types or catch
    :class:`LHPError`.
    """

    _category = ErrorCategory.CONFIG
    _code_number = "025"
    _title_template = "Bundle configuration error"
    _default_suggestions = (
        "Review your databricks.yml configuration",
        "Check the Databricks Asset Bundles documentation",
        "Run 'lhp validate' to check for configuration issues",
    )

    def __init__(
        self,
        message: str,
        original_error: Optional[Exception] = None,
    ):
        """Initialize a bundle configuration error.

        Args:
            message: Human-readable error message
            original_error: Underlying exception (legacy attribute,
                preserved for ``convert_bundle_error`` compatibility)
        """
        super().__init__(
            category=self._category,
            code_number=self._code_number,
            title=self._title_template,
            details=message,
            suggestions=list(self._default_suggestions),
            context={},
            example=None,
            doc_link=None,
        )
        self.original_error = original_error

    def __reduce__(self):
        return (
            self.__class__,
            (self.details, self.original_error),
        )
