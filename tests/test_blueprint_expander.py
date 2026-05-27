"""Spec-driven unit tests for BlueprintExpander (Phase 5).

Covers M1 (env-token rejection, code 044), M6 (variables precedence),
B6 (duplicate-tuple detection, code 045), 055 (unresolved %{var}),
parameter defaults, and Cartesian-product expansion.
"""

from pathlib import Path

import pytest

from lhp.core.processing.blueprint_expander import BlueprintExpander, BlueprintProvenance
from lhp.models.config import (
    Blueprint,
    BlueprintFlowgroupSpec,
    BlueprintInstance,
    BlueprintParameter,
)
from lhp.errors import ErrorCategory, LHPError

pytestmark = pytest.mark.unit


# ---------------------------------------------------------------------------
# Helpers — pure model construction (no YAML round-trip needed for the unit).
# ---------------------------------------------------------------------------


def _make_blueprint(
    name: str = "erp",
    parameters=None,
    flowgroups=None,
):
    return Blueprint(
        name=name,
        parameters=parameters or [],
        flowgroups=flowgroups
        or [
            BlueprintFlowgroupSpec(
                pipeline="%{site_name}_raw",
                flowgroup="%{site_name}_orders",
                actions=[],
            )
        ],
    )


def _make_instance(blueprint="erp", **params) -> BlueprintInstance:
    return BlueprintInstance(blueprint=blueprint, **params)


# ---------------------------------------------------------------------------
# Cartesian product correctness
# ---------------------------------------------------------------------------


def test_cartesian_product_two_specs_three_instances(tmp_path):
    bp = _make_blueprint(
        parameters=[BlueprintParameter(name="site_name", required=True)],
        flowgroups=[
            BlueprintFlowgroupSpec(
                pipeline="%{site_name}_raw",
                flowgroup="%{site_name}_orders",
                actions=[],
            ),
            BlueprintFlowgroupSpec(
                pipeline="%{site_name}_raw",
                flowgroup="%{site_name}_customers",
                actions=[],
            ),
        ],
    )
    bp_path = tmp_path / "blueprints" / "erp.yaml"
    bp_path.parent.mkdir(parents=True, exist_ok=True)
    bp_path.touch()
    blueprints = {"erp": (bp, bp_path)}
    instances = []
    for site in ["apac_sg", "emea_uk", "latam_br"]:
        ip = tmp_path / "instances" / f"{site}.yaml"
        ip.parent.mkdir(parents=True, exist_ok=True)
        ip.touch()
        instances.append((_make_instance(site_name=site), ip))
    contexts, provenance = BlueprintExpander().expand(blueprints, instances)
    # 2 specs × 3 instances = 6 expanded flowgroup contexts.
    assert len(contexts) == 6
    # All synthetic and have correct resolved tuples.
    pipelines = {ctx.flowgroup.pipeline for ctx in contexts}
    flow_names = {ctx.flowgroup.flowgroup for ctx in contexts}
    assert pipelines == {"apac_sg_raw", "emea_uk_raw", "latam_br_raw"}
    assert "apac_sg_orders" in flow_names
    assert "latam_br_customers" in flow_names
    for ctx in contexts:
        assert ctx.synthetic is True
    # Provenance map keyed by resolved tuples.
    assert ("apac_sg_raw", "apac_sg_orders") in provenance
    assert ("latam_br_raw", "latam_br_customers") in provenance
    prov = provenance[("apac_sg_raw", "apac_sg_orders")]
    assert isinstance(prov, BlueprintProvenance)
    assert prov.blueprint_name == "erp"
    assert prov.blueprint_path == bp_path


# ---------------------------------------------------------------------------
# Parameter defaults respected
# ---------------------------------------------------------------------------


def test_default_parameter_used_when_instance_omits(tmp_path):
    bp = _make_blueprint(
        parameters=[
            BlueprintParameter(name="site_name", required=True),
            BlueprintParameter(
                name="partition_key", required=False, default="order_date"
            ),
        ],
        flowgroups=[
            BlueprintFlowgroupSpec(
                pipeline="%{site_name}_raw",
                flowgroup="%{site_name}_orders",
                variables={"pk": "%{partition_key}"},
                actions=[],
            )
        ],
    )
    bp_path = tmp_path / "blueprints" / "erp.yaml"
    bp_path.parent.mkdir(parents=True, exist_ok=True)
    bp_path.touch()
    inst_path = tmp_path / "instances" / "sg.yaml"
    inst_path.parent.mkdir(parents=True, exist_ok=True)
    inst_path.touch()
    contexts, _ = BlueprintExpander().expand(
        {"erp": (bp, bp_path)},
        [(_make_instance(site_name="apac_sg"), inst_path)],
    )
    fg = contexts[0].flowgroup
    # The merged variables should carry the default through to the FlowGroup.
    assert fg.variables is not None
    # Either the raw `pk` template uses the default, or the merged dict carries
    # the default for downstream Step 0.5 resolution.
    assert "partition_key" in fg.variables
    assert fg.variables["partition_key"] == "order_date"


# ---------------------------------------------------------------------------
# M1: ${env_token} in identity fields raises code 044
# ---------------------------------------------------------------------------


def test_env_token_in_pipeline_raises_044(tmp_path):
    bp = _make_blueprint(
        parameters=[BlueprintParameter(name="site_name", required=True)],
        flowgroups=[
            BlueprintFlowgroupSpec(
                pipeline="${env}_${site_name}_raw",
                flowgroup="%{site_name}_orders",
                actions=[],
            )
        ],
    )
    bp_path = tmp_path / "blueprints" / "erp.yaml"
    bp_path.parent.mkdir(parents=True, exist_ok=True)
    bp_path.touch()
    inst_path = tmp_path / "instances" / "sg.yaml"
    inst_path.parent.mkdir(parents=True, exist_ok=True)
    inst_path.touch()
    with pytest.raises(LHPError) as exc:
        BlueprintExpander().expand(
            {"erp": (bp, bp_path)},
            [(_make_instance(site_name="apac_sg"), inst_path)],
        )
    assert exc.value.code == "LHP-VAL-044"
    assert exc.value.category == ErrorCategory.VALIDATION


def test_env_token_in_flowgroup_raises_044(tmp_path):
    bp = _make_blueprint(
        parameters=[BlueprintParameter(name="site_name", required=True)],
        flowgroups=[
            BlueprintFlowgroupSpec(
                pipeline="%{site_name}_raw",
                flowgroup="${something}_orders",
                actions=[],
            )
        ],
    )
    bp_path = tmp_path / "blueprints" / "erp.yaml"
    bp_path.parent.mkdir(parents=True, exist_ok=True)
    bp_path.touch()
    inst_path = tmp_path / "instances" / "sg.yaml"
    inst_path.parent.mkdir(parents=True, exist_ok=True)
    inst_path.touch()
    with pytest.raises(LHPError) as exc:
        BlueprintExpander().expand(
            {"erp": (bp, bp_path)},
            [(_make_instance(site_name="apac_sg"), inst_path)],
        )
    assert exc.value.code == "LHP-VAL-044"


# ---------------------------------------------------------------------------
# M6: variables precedence — spec.variables wins over instance params
# ---------------------------------------------------------------------------


def test_spec_variables_shadow_instance_params(tmp_path):
    bp = _make_blueprint(
        parameters=[
            BlueprintParameter(name="site_name", required=True),
            # Instance is allowed to set `raw_table`, but spec.variables wins.
            BlueprintParameter(
                name="raw_table", required=False, default="instance_default"
            ),
        ],
        flowgroups=[
            BlueprintFlowgroupSpec(
                pipeline="%{site_name}_raw",
                flowgroup="%{site_name}_orders",
                variables={"raw_table": "spec_wins_%{site_name}"},
                actions=[],
            )
        ],
    )
    bp_path = tmp_path / "blueprints" / "erp.yaml"
    bp_path.parent.mkdir(parents=True, exist_ok=True)
    bp_path.touch()
    inst_path = tmp_path / "instances" / "sg.yaml"
    inst_path.parent.mkdir(parents=True, exist_ok=True)
    inst_path.touch()
    # Instance attempts to shadow the spec-defined variable.
    contexts, _ = BlueprintExpander().expand(
        {"erp": (bp, bp_path)},
        [(_make_instance(site_name="apac_sg", raw_table="from_instance"), inst_path)],
    )
    fg = contexts[0].flowgroup
    # Spec value must win — instance 'raw_table=from_instance' must NOT appear.
    assert fg.variables["raw_table"] != "from_instance"
    assert "spec_wins" in fg.variables["raw_table"]


# ---------------------------------------------------------------------------
# B6: duplicate (pipeline, flowgroup) raises 045 with both paths
# ---------------------------------------------------------------------------


def test_duplicate_tuple_raises_045_with_both_paths(tmp_path):
    bp = _make_blueprint(
        parameters=[BlueprintParameter(name="site_name", required=True)],
        flowgroups=[
            BlueprintFlowgroupSpec(
                pipeline="%{site_name}_raw",
                flowgroup="%{site_name}_orders",
                actions=[],
            )
        ],
    )
    bp_path = tmp_path / "blueprints" / "erp.yaml"
    bp_path.parent.mkdir(parents=True, exist_ok=True)
    bp_path.touch()
    inst_a = tmp_path / "instances" / "sg.yaml"
    inst_a.parent.mkdir(parents=True, exist_ok=True)
    inst_a.touch()
    inst_b = tmp_path / "instances" / "sg_duplicate.yaml"
    inst_b.touch()
    # Both instances pick the same site_name, which yields the same expanded tuple.
    with pytest.raises(LHPError) as exc:
        BlueprintExpander().expand(
            {"erp": (bp, bp_path)},
            [
                (_make_instance(site_name="apac_sg"), inst_a),
                (_make_instance(site_name="apac_sg"), inst_b),
            ],
        )
    assert exc.value.code == "LHP-VAL-045"
    msg = str(exc.value)
    assert "sg.yaml" in msg
    assert "sg_duplicate.yaml" in msg
    # Context should also reference both files.
    ctx = exc.value.context
    assert "instance_a" in ctx and "instance_b" in ctx


# ---------------------------------------------------------------------------
# 055: unresolved %{var} in identity fields after merge
# ---------------------------------------------------------------------------


def test_unresolved_var_in_pipeline_raises_055(tmp_path):
    bp = _make_blueprint(
        parameters=[BlueprintParameter(name="site_name", required=True)],
        flowgroups=[
            BlueprintFlowgroupSpec(
                # `region` is not declared as a parameter and not in spec.variables
                # → after merge it remains unresolved in the identity field.
                pipeline="%{region}_%{site_name}_raw",
                flowgroup="%{site_name}_orders",
                actions=[],
            )
        ],
    )
    bp_path = tmp_path / "blueprints" / "erp.yaml"
    bp_path.parent.mkdir(parents=True, exist_ok=True)
    bp_path.touch()
    inst_path = tmp_path / "instances" / "sg.yaml"
    inst_path.parent.mkdir(parents=True, exist_ok=True)
    inst_path.touch()
    with pytest.raises(LHPError) as exc:
        BlueprintExpander().expand(
            {"erp": (bp, bp_path)},
            [(_make_instance(site_name="apac_sg"), inst_path)],
        )
    assert exc.value.code == "LHP-VAL-055"


# ---------------------------------------------------------------------------
# Provenance map structure
# ---------------------------------------------------------------------------


def test_provenance_records_paths(tmp_path):
    bp = _make_blueprint(
        parameters=[BlueprintParameter(name="site_name", required=True)],
        flowgroups=[
            BlueprintFlowgroupSpec(
                pipeline="%{site_name}_raw",
                flowgroup="%{site_name}_orders",
                actions=[],
            )
        ],
    )
    bp_path = tmp_path / "blueprints" / "erp.yaml"
    bp_path.parent.mkdir(parents=True, exist_ok=True)
    bp_path.touch()
    inst_path = tmp_path / "instances" / "sg.yaml"
    inst_path.parent.mkdir(parents=True, exist_ok=True)
    inst_path.touch()
    _, provenance = BlueprintExpander().expand(
        {"erp": (bp, bp_path)},
        [(_make_instance(site_name="apac_sg"), inst_path)],
    )
    key = ("apac_sg_raw", "apac_sg_orders")
    assert key in provenance
    prov = provenance[key]
    assert prov.blueprint_path == bp_path
    assert prov.instance_path == inst_path
    assert prov.spec_index == 0


# ---------------------------------------------------------------------------
# Empty inputs
# ---------------------------------------------------------------------------


def test_empty_inputs_return_empty_outputs():
    contexts, provenance = BlueprintExpander().expand({}, [])
    assert contexts == []
    assert provenance == {}
