"""Unit tests for the unresolved-``${token}`` check on emitted project settings (#298).

Covers :func:`unresolved_setting_token_errors` over plain values: the resolved
bundle settings of each pipeline (``pipeline_config.yaml`` values and the
project ``event_log`` injected from ``lhp.yaml``) and the resolved monitoring
notebook context. One ``LHP-CFG-010`` is built per location, naming the file
and the key; a location shared by several pipelines is reported once.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from lhp.core.validators import PipelineSettings, unresolved_setting_token_errors
from lhp.errors import LHPConfigError

pytestmark = pytest.mark.unit

_PC = "config/pipeline_config.yaml"


def _settings(name, resolved, raw=None):
    return PipelineSettings(name=name, resolved=resolved, raw=raw or {})


def _errors(**kwargs):
    kwargs.setdefault("env", "dev")
    kwargs.setdefault("pipeline_config_file", _PC)
    return unresolved_setting_token_errors(**kwargs)


def _only(errors):
    assert len(errors) == 1, [e.context for e in errors]
    return errors[0]


class TestPipelineSettings:
    def test_undefined_token_in_a_setting_is_cfg_010(self):
        err = _only(
            _errors(
                pipeline_settings=[
                    _settings("bronze", {"catalog": "main", "schema": "${x}"})
                ]
            )
        )

        assert isinstance(err, LHPConfigError)
        assert err.code == "LHP-CFG-010"
        assert err.context["File"] == _PC
        assert err.context["Field"] == "schema"
        assert err.context["Unresolved"] == "${x}"
        assert err.context["Pipeline"] == "bronze"
        assert err.context["Environment"] == "dev"
        assert "Column" not in err.context
        assert "Expression" not in err.context
        assert "substitutions/dev.yaml" in " ".join(err.suggestions)

    def test_nested_and_list_settings_are_named_by_path(self):
        errors = _errors(
            pipeline_settings=[
                _settings(
                    "bronze",
                    {
                        "configuration": {"source_root": "/Volumes/${root}"},
                        "notifications": [
                            {"email_recipients": ["ops@x.com", "${oncall}"]}
                        ],
                    },
                )
            ]
        )

        fields = {e.context["Field"]: e.context["Unresolved"] for e in errors}
        assert fields == {
            "configuration.source_root": "${root}",
            "notifications[0].email_recipients[1]": "${oncall}",
        }

    def test_same_location_in_several_pipelines_is_reported_once(self):
        errors = _errors(
            pipeline_settings=[
                _settings("bronze", {"schema": "${x}"}),
                _settings("silver", {"schema": "${x}"}),
            ]
        )

        err = _only(errors)
        assert err.context["Pipeline"] == "bronze, silver"

    def test_long_pipeline_list_is_capped(self):
        names = [f"p{index}" for index in range(1, 8)]
        errors = _errors(
            pipeline_settings=[_settings(name, {"schema": "${x}"}) for name in names]
        )

        err = _only(errors)
        assert err.context["Pipeline"] == "p1, p2, p3, p4, p5 (+2 more)"

    def test_title_names_the_tokens_and_their_location(self):
        err = _only(
            _errors(pipeline_settings=[_settings("bronze", {"schema": "${a}_${b}"})])
        )

        # ``lhp validate`` prints only the code and title of a preflight issue.
        assert err.title == (
            f"Unresolved substitution token(s) ${{a}}, ${{b}} in {_PC} (schema)"
        )

    def test_title_shows_an_absolute_config_path_project_relative(self, tmp_path):
        err = _only(
            _errors(
                pipeline_settings=[_settings("bronze", {"schema": "${x}"})],
                pipeline_config_file=tmp_path / "config" / "pc.yaml",
                project_root=tmp_path,
            )
        )

        assert err.title.endswith("in config/pc.yaml (schema)")

    def test_details_carry_no_sql_runtime_note(self):
        err = _only(
            _errors(pipeline_settings=[_settings("bronze", {"schema": "${x}"})])
        )

        assert "still contains ${x} after substitution" in err.details
        assert "left in the generated output as written." in err.details
        assert "SQL" not in err.details
        assert "Spark" not in err.details

    def test_different_tokens_at_one_key_stay_separate(self):
        errors = _errors(
            pipeline_settings=[
                _settings("bronze", {"schema": "${x}"}),
                _settings("silver", {"schema": "${y}"}),
            ]
        )

        assert [(e.context["Pipeline"], e.context["Unresolved"]) for e in errors] == [
            ("bronze", "${x}"),
            ("silver", "${y}"),
        ]

    def test_all_tokens_of_one_value_are_named(self):
        err = _only(
            _errors(pipeline_settings=[_settings("bronze", {"schema": "${a}_${b}"})])
        )

        assert err.context["Unresolved"] == "${a}, ${b}"

    def test_event_log_injected_from_lhp_yaml_names_lhp_yaml(self):
        injected = {"name": "bronze_event_log", "catalog": "main", "schema": "${evt}"}
        errors = _errors(
            pipeline_settings=[
                _settings("bronze", {"schema": "s", "event_log": injected}),
                _settings("silver", {"schema": "s", "event_log": injected}),
            ]
        )

        err = _only(errors)
        assert err.context["File"] == "lhp.yaml"
        assert err.context["Field"] == "event_log"
        assert err.context["Unresolved"] == "${evt}"
        assert "Pipeline" not in err.context

    def test_pipelines_own_event_log_names_the_pipeline_config(self):
        own = {"name": "b_log", "catalog": "main", "schema": "${evt}"}
        err = _only(
            _errors(
                pipeline_settings=[
                    _settings("bronze", {"event_log": own}, raw={"event_log": own})
                ]
            )
        )

        assert err.context["File"] == _PC
        assert err.context["Field"] == "event_log.schema"
        assert err.context["Pipeline"] == "bronze"

    @pytest.mark.parametrize(
        "value",
        [
            "${var.catalog}",
            "${bundle.target}",
            "${workspace.file_path}/x",
            "${resources.pipelines.p.id}",
            "${hiveconf:x}",
            "{legacy}",
            "plain",
        ],
    )
    def test_dotted_prefixed_and_legacy_references_pass(self, value):
        assert _errors(pipeline_settings=[_settings("bronze", {"schema": value})]) == []

    def test_non_string_values_and_keys_are_not_checked(self):
        settings = {"serverless": True, "photon": 1, "configuration": {"${k}": "v"}}

        assert _errors(pipeline_settings=[_settings("bronze", settings)]) == []

    def test_absolute_pipeline_config_path_is_shown_project_relative(self, tmp_path):
        err = _only(
            _errors(
                pipeline_settings=[_settings("bronze", {"schema": "${x}"})],
                pipeline_config_file=tmp_path / "config" / "pc.yaml",
                project_root=tmp_path,
            )
        )

        assert err.context["File"] == "config/pc.yaml"


class TestMonitoringNotebook:
    @staticmethod
    def _context(**overrides):
        context = {
            "sources": [["bronze", "main._meta.bronze_event_log"]],
            "target_fqn": "main._meta.all_pipelines_event_log",
            "checkpoint_path": "/Volumes/main/_meta/checkpoints",
            "max_concurrent_streams": 10,
        }
        context.update(overrides)
        return context

    def test_resolved_context_passes(self):
        assert _errors(notebook_context=self._context()) == []

    def test_checkpoint_path_token_names_the_monitoring_field(self):
        err = _only(
            _errors(
                notebook_context=self._context(
                    checkpoint_path="/Volumes/main/${ckpt}/checkpoints"
                )
            )
        )

        assert err.code == "LHP-CFG-010"
        assert err.context["File"] == "lhp.yaml"
        assert err.context["Field"] == "monitoring.checkpoint_path"
        assert err.context["Unresolved"] == "${ckpt}"
        assert err.title == (
            "Unresolved substitution token(s) ${ckpt} in lhp.yaml "
            "(monitoring.checkpoint_path)"
        )
        assert "SQL" not in err.details

    def test_event_log_reference_token_names_event_log(self):
        err = _only(
            _errors(
                notebook_context=self._context(
                    sources=[
                        ["bronze", "main.${evt}.bronze_event_log"],
                        ["silver", "main.${evt}.silver_event_log"],
                    ]
                )
            )
        )

        assert err.context["File"] == "lhp.yaml"
        assert err.context["Field"] == "event_log"
        assert err.context["Unresolved"] == "${evt}"

    def test_target_table_token_inherited_from_event_log_is_not_repeated(self):
        # Without monitoring.catalog/schema the target table uses event_log's.
        err = _only(
            _errors(
                notebook_context=self._context(
                    sources=[["bronze", "main.${evt}.bronze_event_log"]],
                    target_fqn="main.${evt}.all_pipelines_event_log",
                )
            )
        )

        assert err.context["Field"] == "event_log"

    def test_target_table_token_of_its_own_names_the_monitoring_fields(self):
        err = _only(
            _errors(notebook_context=self._context(target_fqn="mon.${sch}.events"))
        )

        assert err.context["File"] == "lhp.yaml"
        assert err.context["Field"].startswith("monitoring.")
        assert "monitoring.streaming_table" in err.context["Field"]
        assert err.context["Unresolved"] == "${sch}"

    def test_dotted_references_pass(self):
        context = self._context(checkpoint_path="/Volumes/${bundle.target}/ckpt")

        assert _errors(notebook_context=context) == []

    def test_event_log_shared_by_bundle_and_notebook_is_reported_once(self):
        injected = {"name": "bronze_log", "catalog": "main", "schema": "${evt}"}
        errors = _errors(
            pipeline_settings=[_settings("bronze", {"event_log": injected})],
            notebook_context=self._context(
                sources=[["bronze", "main.${evt}.bronze_log"]]
            ),
        )

        err = _only(errors)
        assert (err.context["File"], err.context["Field"]) == ("lhp.yaml", "event_log")


def test_nothing_to_check_returns_no_errors():
    assert unresolved_setting_token_errors(env="dev") == []


def test_project_root_accepts_a_path(tmp_path: Path):
    errors = unresolved_setting_token_errors(
        env="prod",
        project_root=tmp_path,
        notebook_context={"checkpoint_path": "/Volumes/${x}"},
    )

    err = _only(errors)
    assert err.context["Environment"] == "prod"
    assert "substitutions/prod.yaml" in " ".join(err.suggestions)
