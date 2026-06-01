"""Unit tests for the snapshot-CDC source-function resolver.

These tests cover the module-level helpers in
``lhp.generators.write.snapshot_cdc_source_function`` directly, without going
through the StreamingTableWriteGenerator. End-to-end coverage of the
resolver through the generator lives in
``tests/unit/test_snapshot_cdc_parameters.py``.
"""

import ast
import tempfile
from pathlib import Path

import pytest

from lhp.errors import LHPError
from lhp.generators.write import snapshot_cdc_source_function as mod
from lhp.generators.write.snapshot_cdc_source_function import (
    FunctionSignature,
    SourceFunctionResult,
    _validate_function_parameters,
    resolve_source_function,
)


def _write_source(tmpdir: str, name: str, body: str) -> str:
    """Write a source file under tmpdir and return its (relative) name."""
    path = Path(tmpdir) / name
    path.write_text(body, encoding="utf-8")
    return name


# NOTE: The old TestResolveSourceFunctionConfigValidation class (which
# asserted a CONFIG/002 presence raise for a missing 'file'/'function')
# was removed. That generate-side guard was redundant: the snapshot-CDC
# validator (SnapshotCdcConfigValidator, reached via
# ConfigValidator.validate_flowgroup → WriteActionValidator) enforces both
# fields and runs UNCONDITIONALLY in the per-flowgroup worker before the
# codegen branch in BOTH validate and generate modes. The generate-path
# rejection is now proven by
# tests/test_generate_command_parallel.py::TestGenerateCommandParallel::test_snapshot_cdc_missing_source_function_function_rejected_at_generate.


class TestResolveSourceFunctionFileNotFound:
    """File-not-found resolution delegates to external_file_loader.

    The shared ``ErrorFormatter.file_not_found`` raises IO/001 (not IO/004)
    for a missing FILE; IO/004 is reserved for a missing FUNCTION inside an
    existing file (see TestSyntaxAndMissingFunction).
    """

    def test_missing_file_raises_lhp_error_with_file_type(self):
        """A non-existent file raises an LHPError mentioning the file_type."""
        with tempfile.TemporaryDirectory() as tmpdir:
            with pytest.raises(LHPError) as exc_info:
                resolve_source_function(
                    {"file": "does_not_exist.py", "function": "my_func"},
                    context={"project_root": Path(tmpdir)},
                )
            assert "snapshot source function file" in str(exc_info.value)


class TestResolveSourceFunctionResult:
    """resolve_source_function returns the expected (name, parameters)."""

    def test_returns_name_and_parameters(self):
        body = "def my_func(latest, *, catalog, schema):\n    return None\n"
        with tempfile.TemporaryDirectory() as tmpdir:
            file_name = _write_source(tmpdir, "funcs.py", body)
            result = resolve_source_function(
                {
                    "file": file_name,
                    "function": "my_func",
                    "parameters": {"catalog": "c", "schema": "s"},
                },
                context={
                    "project_root": Path(tmpdir),
                    "source_function_signature_cache": {},
                },
            )
        assert isinstance(result, SourceFunctionResult)
        assert result.name == "my_func"
        assert result.parameters == {"catalog": "c", "schema": "s"}
        assert not hasattr(result, "code")
        assert result._fields == ("name", "parameters")

    def test_returns_none_parameters_when_absent(self):
        body = "def my_func(latest):\n    return None\n"
        with tempfile.TemporaryDirectory() as tmpdir:
            file_name = _write_source(tmpdir, "funcs.py", body)
            result = resolve_source_function(
                {"file": file_name, "function": "my_func"},
                context={
                    "project_root": Path(tmpdir),
                    "source_function_signature_cache": {},
                },
            )
        assert result.parameters is None


class TestPerPipelineSignatureCache:
    """GATED INVARIANT: a source file is parsed at most once per pipeline.

    If per-flowgroup parsing is reintroduced, the second call re-parses
    and this test fails on the ast.parse call count.
    """

    def test_same_file_parsed_only_once(self, monkeypatch):
        body = "def my_func(latest, *, catalog):\n    return None\n"
        parse_calls = {"n": 0}
        real_parse = ast.parse

        def counting_parse(*args, **kwargs):
            parse_calls["n"] += 1
            return real_parse(*args, **kwargs)

        monkeypatch.setattr(mod.ast, "parse", counting_parse)

        with tempfile.TemporaryDirectory() as tmpdir:
            file_name = _write_source(tmpdir, "funcs.py", body)
            shared_cache: dict = {}
            ctx = {
                "project_root": Path(tmpdir),
                "source_function_signature_cache": shared_cache,
            }
            # Two flowgroups binding to the SAME file (distinct param sets).
            resolve_source_function(
                {
                    "file": file_name,
                    "function": "my_func",
                    "parameters": {"catalog": "a"},
                },
                context=ctx,
            )
            resolve_source_function(
                {
                    "file": file_name,
                    "function": "my_func",
                    "parameters": {"catalog": "b"},
                },
                context=ctx,
            )

        assert parse_calls["n"] == 1, (
            "Source file must be parsed exactly once per pipeline; "
            "per-flowgroup re-parsing was reintroduced."
        )
        # The cache holds a FunctionSignature keyed by resolved absolute path.
        (key,) = shared_cache.keys()
        assert Path(key).is_absolute()
        assert isinstance(shared_cache[key], FunctionSignature)

    def test_cache_hit_skips_reparse_via_counter(self, monkeypatch):
        """Second call with the cache pre-populated does not parse at all."""
        body = "def my_func(latest, *, catalog):\n    return None\n"
        parse_calls = {"n": 0}
        real_parse = ast.parse

        def counting_parse(*args, **kwargs):
            parse_calls["n"] += 1
            return real_parse(*args, **kwargs)

        with tempfile.TemporaryDirectory() as tmpdir:
            file_name = _write_source(tmpdir, "funcs.py", body)
            shared_cache: dict = {}
            ctx = {
                "project_root": Path(tmpdir),
                "source_function_signature_cache": shared_cache,
            }
            # Warm the cache (real parse).
            resolve_source_function(
                {"file": file_name, "function": "my_func"},
                context=ctx,
            )
            # Now count: a cache hit must not parse.
            monkeypatch.setattr(mod.ast, "parse", counting_parse)
            resolve_source_function(
                {"file": file_name, "function": "my_func"},
                context=ctx,
            )
        assert parse_calls["n"] == 0


class TestParameterValidationErrors:
    """Parameter validation through resolve_source_function (CONFIG/005, /006)."""

    def test_unknown_param_name_raises_config_006(self):
        body = "def my_func(latest, *, catalog):\n    return None\n"
        with tempfile.TemporaryDirectory() as tmpdir:
            file_name = _write_source(tmpdir, "funcs.py", body)
            with pytest.raises(LHPError) as exc_info:
                resolve_source_function(
                    {
                        "file": file_name,
                        "function": "my_func",
                        "parameters": {"not_a_param": "x"},
                    },
                    context={
                        "project_root": Path(tmpdir),
                        "source_function_signature_cache": {},
                    },
                )
        assert exc_info.value.code_number == "006"

    def test_unsupported_param_type_raises_config_005(self):
        body = "def my_func(latest, *, catalog):\n    return None\n"

        class Weird:
            pass

        with tempfile.TemporaryDirectory() as tmpdir:
            file_name = _write_source(tmpdir, "funcs.py", body)
            with pytest.raises(LHPError) as exc_info:
                resolve_source_function(
                    {
                        "file": file_name,
                        "function": "my_func",
                        "parameters": {"catalog": Weird()},
                    },
                    context={
                        "project_root": Path(tmpdir),
                        "source_function_signature_cache": {},
                    },
                )
        assert exc_info.value.code_number == "005"

    def test_kwargs_function_skips_name_validation(self):
        body = "def flexible(latest, **kwargs):\n    return None\n"
        with tempfile.TemporaryDirectory() as tmpdir:
            file_name = _write_source(tmpdir, "funcs.py", body)
            result = resolve_source_function(
                {
                    "file": file_name,
                    "function": "flexible",
                    "parameters": {"anything": "value"},
                },
                context={
                    "project_root": Path(tmpdir),
                    "source_function_signature_cache": {},
                },
            )
        assert result.parameters == {"anything": "value"}


class TestSyntaxAndMissingFunction:
    """AST-level errors surface as IO/003 (syntax) and IO/004 (not found)."""

    def test_syntax_error_raises_io_003(self):
        body = "def my_func(latest, *, catalog\n    return None\n"  # missing ')'
        with tempfile.TemporaryDirectory() as tmpdir:
            file_name = _write_source(tmpdir, "funcs.py", body)
            with pytest.raises(LHPError) as exc_info:
                resolve_source_function(
                    {"file": file_name, "function": "my_func"},
                    context={
                        "project_root": Path(tmpdir),
                        "source_function_signature_cache": {},
                    },
                )
        assert exc_info.value.code_number == "003"

    def test_missing_function_in_file_raises_io_004(self):
        body = "def other_func():\n    return None\n"
        with tempfile.TemporaryDirectory() as tmpdir:
            file_name = _write_source(tmpdir, "funcs.py", body)
            with pytest.raises(LHPError) as exc_info:
                resolve_source_function(
                    {"file": file_name, "function": "my_func"},
                    context={
                        "project_root": Path(tmpdir),
                        "source_function_signature_cache": {},
                    },
                )
        assert exc_info.value.code_number == "004"


class TestSignatureCacheEventCounters:
    """The resolver increments perf event counters on cache miss / hit.

    Instrumentation only: ``incr_event`` is a no-op unless perf timing is
    enabled, so these counts are observable only inside an
    ``enable_perf_timing`` window. The E2E fixture cannot prove a HIT (its
    two snapshot-CDC flowgroups reference different source files, so each
    is a miss), hence this focused in-process test.
    """

    @pytest.fixture
    def _perf_window(self, tmp_path):
        """Enable perf timing for the test and fully reset it afterward.

        Mirrors the reset pattern in tests/unit/test_performance_timer.py so
        the module-level singleton does not leak into other tests.
        """
        import lhp.utils.performance_timer as pt

        pt._enabled = False
        pt._start_wall_clock = None
        pt._summary.reset()
        for handler in pt._perf_logger.handlers[:]:
            handler.close()
            pt._perf_logger.removeHandler(handler)

        pt.enable_perf_timing(tmp_path)
        yield pt

        pt._enabled = False
        pt._start_wall_clock = None
        pt._summary.reset()
        for handler in pt._perf_logger.handlers[:]:
            handler.close()
            pt._perf_logger.removeHandler(handler)

    def test_miss_then_hit_counts(self, _perf_window, tmp_path):
        """Same file + shared cache: 1st call misses, 2nd call hits.

        ``snapshot()`` (get_perf_summary) does not expose event counts; the
        public inspection path for events is ``export_perf_for_merge``.
        """
        pt = _perf_window
        body = "def my_func(latest, *, catalog):\n    return None\n"
        file_name = _write_source(str(tmp_path), "funcs.py", body)

        shared_cache: dict = {}
        ctx = {
            "project_root": Path(tmp_path),
            "source_function_signature_cache": shared_cache,
        }

        # First call: cache empty -> MISS (parse runs, signature stored).
        resolve_source_function(
            {"file": file_name, "function": "my_func"},
            context=ctx,
        )
        # Second call: same file, populated cache -> HIT (no parse).
        resolve_source_function(
            {"file": file_name, "function": "my_func"},
            context=ctx,
        )

        events = pt.export_perf_for_merge()["events"]
        assert events.get("snapshot_sigcache_miss") == 1
        assert events.get("snapshot_sigcache_hit") == 1


class TestValidateFunctionParametersDirect:
    """Direct unit tests for _validate_function_parameters on FunctionSignature."""

    def test_keyword_only_args_accepted(self):
        sig = FunctionSignature(
            kwonly_names=frozenset({"catalog", "schema"}), has_kwargs=False
        )
        # Should not raise.
        _validate_function_parameters(sig, "my_func", {"catalog": "c", "schema": "s"})

    def test_kwargs_signature_skips_name_validation(self):
        sig = FunctionSignature(kwonly_names=frozenset(), has_kwargs=True)
        _validate_function_parameters(sig, "flexible", {"anything": "value"})
