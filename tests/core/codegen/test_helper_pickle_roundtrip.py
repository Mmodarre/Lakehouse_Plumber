"""Pickle-by-value round-trip for a custom data source that imports a helper.

LHP emits ``register_pickle_by_value(custom_python_functions)`` for the
**top-level** package (``custom_datasource.py`` / ``custom_sink.py``). The
transitive-helper feature places copied helpers *under*
``custom_python_functions/helpers/...``. cloudpickle's registry check walks the
parent-package chain, so a class living in ``custom_python_functions.my_source``
that calls a helper in ``custom_python_functions.helpers.util`` must travel to
executors **by value** with no change to that single registration line.

This test proves that contract end-to-end on an inline, self-contained package
(no dependency on the T5 e2e fixture): it registers only the top package,
pickles an instance, then makes ``custom_python_functions`` genuinely
unimportable — off both ``sys.modules`` and ``sys.path`` — before unpickling.
Under those conditions a by-reference pickle raises ``ModuleNotFoundError``;
success therefore means the class *and* its helper were embedded by value.
"""

from __future__ import annotations

from pathlib import Path

import pytest

# The runtime path uses PySpark's vendored cloudpickle
# (``from pyspark import cloudpickle``); the standalone ``cloudpickle`` package
# is the same upstream library and exposes the identical ``register_pickle_by_value``
# / ``dumps`` / ``loads`` surface, so it is a faithful stand-in here.
cloudpickle = pytest.importorskip("cloudpickle")

PACKAGE_NAME = "custom_python_functions"

# Submodule names purged before unpickling so a by-reference pickle would fail.
_PURGE_MODULES = (
    f"{PACKAGE_NAME}.my_source",
    f"{PACKAGE_NAME}.helpers.util",
    f"{PACKAGE_NAME}.helpers",
    PACKAGE_NAME,
)

_BY_REFERENCE_FAILURE = (
    "cloudpickle.loads raised ModuleNotFoundError with "
    f"'{PACKAGE_NAME}' off sys.path/sys.modules. This means the object was "
    "pickled BY REFERENCE, not by value: register_pickle_by_value on the top "
    "package did not embed the helper sub-package code. The transitive-helper "
    "layout (helpers under custom_python_functions/) relies on cloudpickle's "
    "parent-package walk to carry descendants by value."
)


def _build_package(root: Path) -> None:
    """Lay down a minimal ``custom_python_functions`` package under *root*.

    Mirrors what LHP emits: an entry module whose method imports a helper via
    the absolute, prefix-rewritten form ``from custom_python_functions.helpers.util
    import transform``, plus the helper sub-package itself.
    """
    pkg = root / PACKAGE_NAME
    helpers = pkg / "helpers"
    helpers.mkdir(parents=True)

    (pkg / "__init__.py").write_text("")
    (helpers / "__init__.py").write_text("")

    # The helper carries the behavior we assert survived the by-value transport.
    (helpers / "util.py").write_text(
        "def transform(value):\n    return value * 3 + 1\n"
    )

    # A tiny stand-in for a custom data source class: its method reaches into
    # the helper sub-package via the absolute, prefix-rewritten import LHP emits.
    (pkg / "my_source.py").write_text(
        "from custom_python_functions.helpers.util import transform\n"
        "\n"
        "\n"
        "class MySource:\n"
        '    """Stand-in custom data source whose method calls a helper."""\n'
        "\n"
        "    def run(self, value):\n"
        "        return transform(value)\n"
    )


@pytest.mark.integration
class TestHelperPickleByValueRoundTrip:
    """A helper-importing custom source pickles by value to a bare namespace."""

    def test_helper_backed_class_unpickles_without_package_on_path(
        self, tmp_path: Path
    ) -> None:
        import sys

        _build_package(tmp_path)

        root = str(tmp_path)
        saved_path = list(sys.path)
        saved_modules = {name: sys.modules.get(name) for name in _PURGE_MODULES}
        try:
            sys.path.insert(0, root)

            # Built under tmp_path at runtime, so static analysis cannot see it.
            import custom_python_functions  # type: ignore[import-not-found]  # noqa: PLC0415
            from custom_python_functions.my_source import (  # type: ignore[import-not-found]  # noqa: PLC0415
                MySource,
            )

            # Register only the TOP package — exactly the line LHP emits and
            # the line that must stay unchanged for the helper feature.
            cloudpickle.register_pickle_by_value(custom_python_functions)

            instance = MySource()
            blob = cloudpickle.dumps(instance)

            # Actively defeat by-reference: drop the package (and its helper
            # sub-package + submodules) from sys.modules AND remove its root
            # from sys.path, so re-import is impossible at unpickle time.
            for name in _PURGE_MODULES:
                sys.modules.pop(name, None)
            sys.path.remove(root)

            with pytest.raises(ModuleNotFoundError):
                __import__(PACKAGE_NAME)

            restored = cloudpickle.loads(blob)

            assert type(restored).__name__ == "MySource", _BY_REFERENCE_FAILURE
            # The helper's behavior must work with nothing importable: proves
            # the helper code itself travelled by value, not just the class.
            assert restored.run(10) == 31, _BY_REFERENCE_FAILURE
        finally:
            sys.path[:] = saved_path
            for name, module in saved_modules.items():
                if module is None:
                    sys.modules.pop(name, None)
                else:
                    sys.modules[name] = module
