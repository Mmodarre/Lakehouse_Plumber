"""Render the per-pipeline wheel-mode *runner* source.

The runner is the single ``.py`` file synced for a wheel-mode pipeline. It is
NOT part of the wheel: it bootstraps the SDP-ambient globals into ``builtins``
and then imports the wheel's flowgroup package so each flowgroup module's
dataset-registering decorators execute (WHEEL_PACKAGING_SPEC §6.6, R9).

Per constitution §2.10 / §9.14 the runner *body* is a Jinja2 template
(``templates/packaging/runner.py.j2``), never a Python-code-as-string
literal here. Rendering reuses the shared LHP package-template loader
(:class:`lhp.core.codegen.template_renderer.TemplateRenderer`) so this module
constructs no bare Jinja2 ``Environment`` of its own.
"""

from __future__ import annotations

from ..codegen.template_renderer import TemplateRenderer

_RUNNER_TEMPLATE = "packaging/runner.py.j2"

# Ambient globals the Databricks runtime injects into the runner's namespace.
# Extensible if other ambient names surface (WHEEL_PACKAGING_SPEC R9).
_DEFAULT_AMBIENT_GLOBALS: tuple[str, ...] = ("spark", "dbutils")


def build_runner_code(
    *,
    import_package: str,
    ambient_globals: tuple[str, ...] = _DEFAULT_AMBIENT_GLOBALS,
) -> str:
    """Render the runner source for a wheel-mode pipeline.

    ``import_package`` is the flowgroup *import-package* name (the sanitized
    pipeline name) — the only pipeline-specific identity the runner references.
    It deliberately never sees the distribution name, content hash, or version,
    so the rendered bytes are stable across pipeline content changes
    (WHEEL_PACKAGING_SPEC §6.6).

    ``ambient_globals`` is the set of runtime-injected names republished into
    ``builtins`` before any wheel import; it defaults to ``("spark",
    "dbutils")`` and is extensible.
    """
    renderer = TemplateRenderer.from_package()
    return renderer.render_template(
        _RUNNER_TEMPLATE,
        {
            "import_package": import_package,
            "ambient_globals": ambient_globals,
        },
    )
