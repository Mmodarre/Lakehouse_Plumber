"""Concrete picklable fakes for the worker-boundary collaborators.

Each fake mirrors only the surface area the production worker code actually
reads. The aim is to be cheap to construct, deterministic, and safe to pickle
across a ``ProcessPoolExecutor`` ``spawn`` boundary.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class CallRecord:
    """A single captured call to a fake collaborator method.

    ``args`` and ``kwargs`` are stored by reference. The caller must therefore
    pass picklable arguments if the fake will itself cross a process boundary
    with the recorded call still attached.
    """

    args: tuple[Any, ...]
    kwargs: dict[str, Any]


class FakeFlowgroupResolutionService:
    """Stand-in for :class:`lhp.core.processing.flowgroup_resolver.FlowgroupResolutionService`.

    Records each ``process_flowgroup`` invocation in :attr:`calls` and returns
    the flowgroup it received unchanged. Concrete (non-``Mock``) class so
    instances pickle across the ``spawn`` boundary used by
    :class:`concurrent.futures.ProcessPoolExecutor`.
    """

    def __init__(self) -> None:
        self.calls: list[CallRecord] = []

    def process_flowgroup(self, *args: Any, **kwargs: Any) -> Any:
        self.calls.append(CallRecord(args=args, kwargs=kwargs))
        # Return the first positional argument (the flowgroup) so callers that
        # expect a processed flowgroup back receive something compatible.
        return args[0] if args else None


class FakeSubstitutionManager:
    """Stand-in for :class:`lhp.core.processing.substitution.EnhancedSubstitutionManager`.

    Exposes the attributes the production code reads on workers:

    * ``env`` — the environment name used in log/error messages.
    * ``skip_validation`` — gates the unresolved-token validation step.
    * ``secret_references`` — iterable consumed by :class:`SecretValidator`.
    * ``mappings`` — environment substitution map.

    ``substitute_yaml`` returns its input unchanged, matching the no-op
    behaviour of the real manager when no tokens are present.
    """

    def __init__(
        self,
        env: str = "dev",
        *,
        skip_validation: bool = False,
        secret_references: list[Any] | None = None,
        mappings: dict[str, Any] | None = None,
    ) -> None:
        self.env = env
        self.skip_validation = skip_validation
        self.secret_references: list[Any] = (
            list(secret_references) if secret_references is not None else []
        )
        self.mappings: dict[str, Any] = dict(mappings) if mappings is not None else {}

    def substitute_yaml(self, data: Any) -> Any:
        return data

    def validate_no_unresolved_tokens(self, _data: Any) -> list[str]:
        return []


@dataclass
class FakeTemplate:
    """Stand-in for :class:`lhp.models.config.Template`.

    Only the attributes consumed by :class:`FlowgroupResolutionService.process_flowgroup`
    (``presets`` and ``actions``) are modelled. Concrete ``@dataclass`` so
    instances pickle.
    """

    presets: list[str] | None = None
    actions: list[Any] = field(default_factory=list)

    def has_raw_actions(self) -> bool:
        return all(isinstance(a, dict) for a in self.actions) and bool(self.actions)


class FakeTemplateEngine:
    """Stand-in for :class:`lhp.core.processing.template_engine.TemplateEngine`.

    Constructor seeds ``get_template`` and ``render_template`` return values.
    Pickle-safe because every attribute is a concrete picklable type.
    """

    def __init__(
        self,
        *,
        template: FakeTemplate | None = None,
        rendered_actions: list[Any] | None = None,
    ) -> None:
        self._template = template
        self._rendered_actions: list[Any] = (
            list(rendered_actions) if rendered_actions is not None else []
        )

    def get_template(self, _template_name: str) -> FakeTemplate | None:
        return self._template

    def render_template(
        self, _template_name: str, _parameters: dict[str, Any]
    ) -> list[Any]:
        return list(self._rendered_actions)


class FakeCodeGenerator:
    """Stand-in for :class:`lhp.core.codegen.coordinator.CodeGenerationService`.

    Worker boundary tests in this package only need a typed,
    constructor-accepting slot. No methods are exercised by the in-scope
    contract tests; declaring the class without methods keeps the surface
    honest. Add methods here only when a concrete test reads them.
    """


class FakeCodeFormatter:
    """Stand-in for :class:`lhp.core.codegen.formatter.CodeFormatter`.

    See :class:`FakeCodeGenerator` for the deliberate empty-surface rationale.
    """


class FakeProjectConfig:
    """Stand-in for :class:`lhp.models.config.ProjectConfig`.

    Only the fields read on the generate commit path are exposed.
    ``test_reporting`` is the single field consumed opaquely; default
    ``None`` matches projects without a ``test_reporting`` block.
    """

    def __init__(
        self,
        *,
        name: str = "test_project",
        test_reporting: Any | None = None,
    ) -> None:
        self.name = name
        self.test_reporting = test_reporting
