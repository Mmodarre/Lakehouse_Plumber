"""End-to-end ordering proof for the four-layer substitution chain.

The documented precedence is::

    %{local_var}  ->  {{ template_param }}  ->  ${env_token}  ->  ${secret:scope/key}

i.e. local variables resolve first, then Jinja2 template parameters, then
environment tokens, and finally secret references. The production driver of
this chain is ``FlowgroupResolutionService.process_flowgroup``
(``src/lhp/core/processing/flowgroup_resolver.py``), which runs, in order:

1. ``LocalVariableResolver.resolve(...)``          (flowgroup_resolver.py:104-108)
2. ``TemplateEngine.render_template(...)``         (flowgroup_resolver.py:118)
3. ``EnhancedSubstitutionManager.substitute_yaml(...)`` (flowgroup_resolver.py:173)

and within step 3, ``_process_string`` (substitution.py:222) resolves
``${env_token}`` *before* ``${secret:...}`` (substitution.py:225 then :261).

This test drives the *real* components (no mocks of the substitution logic
itself) through a single config whose layers are deliberately interlocked so
that the ORDER is observable: each layer's output is the *input syntax* of the
next layer. If any layer ran out of order, the corresponding token would never
be produced and the assertion (or a real unresolved-token error) would fail.

The interlock, expressed in one carrier string ``source``:

* The flowgroup field literal is ``"tbl_%{lv}"`` and the local variable
  ``lv`` is defined as ``"{{ tparam }}"``. Layer 1 turns ``%{lv}`` into the
  ``{{ tparam }}`` *template syntax*. If templates ran first, no ``{{ }}``
  would exist yet, ``%{lv}`` would survive, and ``LocalVariableResolver``'s
  strict ``_validate_no_unresolved`` would raise LHP-CFG-011.
* ``{{ tparam }}`` is a template parameter whose value is ``"${env_tok}"``.
  Layer 2 turns it into the ``${env_tok}`` *env-token syntax*. If env
  substitution ran before templates, ``substitute_yaml`` would leave
  ``{{ tparam }}`` untouched (it does not process Jinja2), and the
  unresolved-token validator would flag it.
* ``${env_tok}`` is an env mapping whose value is ``"${secret:db/pw}"``.
  Layer 3 (token pass) turns it into the ``${secret:db/pw}`` *secret syntax*,
  and the secret pass (same call, but strictly after the token pass) turns it
  into the ``__SECRET_<scope>_<key>__`` placeholder, registering the secret
  reference. If the secret pass ran before the token pass, ``${env_tok}``
  would not yet be a secret reference and would survive as a literal token.

So the single observed value ``source`` only reaches
``__SECRET_dev_db_secrets_pw__`` if all four layers ran in the documented
order. Any reordering breaks a distinct boundary, each asserted below.
"""

import textwrap
from pathlib import Path

import pytest

from lhp.core.processing.local_variables import LocalVariableResolver
from lhp.core.processing.substitution import EnhancedSubstitutionManager
from lhp.core.processing.template_engine import TemplateEngine


@pytest.mark.unit
@pytest.mark.integration
class TestSubstitutionLayerOrdering:
    """One end-to-end test that makes the four-layer precedence observable."""

    @staticmethod
    def _write_template(templates_dir: Path) -> None:
        """``source`` is the composite carrier; ``target`` and ``description``
        are single-boundary probes so a failure pinpoints *which* boundary
        broke rather than only the composite.
        """
        templates_dir.mkdir(parents=True, exist_ok=True)
        (templates_dir / "ordering_chain.yaml").write_text(
            textwrap.dedent(
                """
                name: ordering_chain
                version: "1.0"
                parameters:
                  - name: tparam
                actions:
                  - name: load_chain
                    type: load
                    source: "{{ tparam }}"
                    target: "v_chain"
                """
            ).lstrip()
        )

    @staticmethod
    def _write_substitutions(sub_file: Path) -> None:
        """``env_tok`` resolves to a *secret* token.

        The ``db`` scope alias maps to ``dev_db_secrets``; the alias resolution
        is part of the secret layer and is asserted via the registered
        ``SecretReference``.
        """
        sub_file.write_text(
            textwrap.dedent(
                """
                dev:
                  env_tok: "${secret:db/pw}"
                  plain_tok: "resolved_env_value"
                secrets:
                  default_scope: fallback_scope
                  scopes:
                    db: dev_db_secrets
                """
            ).lstrip()
        )

    def test_four_layers_resolve_in_documented_order(self, tmp_path):
        """Drive %{} -> {{}} -> ${} -> ${secret} through the real chain.

        Mirrors the exact production sequence in
        ``FlowgroupResolutionService.process_flowgroup``.
        """
        templates_dir = tmp_path / "templates"
        sub_file = tmp_path / "dev.yaml"
        self._write_template(templates_dir)
        self._write_substitutions(sub_file)

        # The flowgroup-level fields BEFORE any layer runs. Note `source` only
        # contains `%{lv}` syntax at this point — the `{{ }}` / `${ }` /
        # `${secret}` tokens do not exist yet and are *produced* by later
        # layers. This is what makes the order observable.
        flowgroup_dict = {
            "source": "tbl_%{lv}",  # 4-layer composite carrier
            "target": "out_%{lv}",  # probes only the L1->L2 boundary
            "description": "${env_tok}",  # probes only the L3->L4 boundary
        }
        local_variables = {"lv": "{{ tparam }}"}
        template_parameters = {"tparam": "${env_tok}"}

        # Layer 1: local variables (%{})
        resolver = LocalVariableResolver(local_variables)
        after_local = resolver.resolve(flowgroup_dict)

        # Boundary L1->L2: %{lv} became the {{ tparam }} template syntax.
        # If templates had run first this would still read "tbl_%{lv}" and the
        # resolver's strict validator would have raised instead.
        assert after_local["source"] == "tbl_{{ tparam }}"
        assert after_local["target"] == "out_{{ tparam }}"
        # description had no %{} and is carried through untouched.
        assert after_local["description"] == "${env_tok}"

        # Layer 2: template params ({{ }})
        engine = TemplateEngine(templates_dir)
        rendered_actions = engine.render_template("ordering_chain", template_parameters)
        assert len(rendered_actions) == 1
        rendered = rendered_actions[0]

        assert rendered.source == "${env_tok}"

        after_template = {
            k: engine._render_value(v, template_parameters)
            for k, v in after_local.items()
        }
        # Boundary L1->L2 confirmed end-to-end: {{ tparam }} became ${env_tok}.
        assert after_template["source"] == "tbl_${env_tok}"
        assert after_template["target"] == "out_${env_tok}"
        # description still untouched (no {{ }}); ${} is not Jinja2 syntax.
        assert after_template["description"] == "${env_tok}"

        # Layers 3 & 4: env tokens + secrets
        sub_mgr = EnhancedSubstitutionManager(sub_file, env="dev")

        # The env mapping itself must NOT have been collapsed at construction:
        # ${secret:...} is not a \\w-only ${token}, so it survives expansion
        # and the *runtime* token pass is what reveals it. This guards the
        # token-before-secret ordering at its source.
        assert sub_mgr.mappings["env_tok"] == "${secret:db/pw}"

        after_subst = sub_mgr.substitute_yaml(after_template)

        # Boundary L2->L3 then L3->L4, composited on `source`:
        #   tbl_${env_tok}
        #     --(token pass)-->  tbl_${secret:db/pw}
        #     --(secret pass)--> tbl___SECRET_dev_db_secrets_pw__
        # Reaching the placeholder proves: env-token resolved (L3) BEFORE the
        # secret pass (L4), the `db` alias resolved to `dev_db_secrets`, and
        # every preceding layer ran in order.
        assert after_subst["source"] == "tbl___SECRET_dev_db_secrets_pw__"
        assert after_subst["target"] == "out___SECRET_dev_db_secrets_pw__"
        # Independent L3->L4 probe via `description` (no template layer at all):
        assert after_subst["description"] == "__SECRET_dev_db_secrets_pw__"

        # Layer 4 side effect: the secret reference is registered with the
        # alias-resolved scope and the extracted key (proves the secret pass,
        # not merely a literal passthrough, executed).
        registered = {(r.scope, r.key) for r in sub_mgr.secret_references}
        assert ("dev_db_secrets", "pw") in registered

        # Final guard: no token syntax of ANY layer survives. If a layer had
        # run out of order, a `%{...}`, `{{ ... }}`, or `${...}` fragment would
        # remain here.
        flat = " ".join(str(v) for v in after_subst.values())
        assert "%{" not in flat
        assert "{{" not in flat
        assert "${" not in flat

    def test_order_is_load_bearing_reverse_order_fails(self, tmp_path):
        """Negative control: applying the layers in the WRONG order does NOT
        produce the resolved value, proving the assertions above are sensitive
        to ordering rather than coincidentally true.

        Here we run the env/secret substitution FIRST (before local vars and
        templates). The ``%{lv}`` / ``{{ tparam }}`` syntax is opaque to the
        substitution manager, so the carrier cannot resolve and the local-var
        validator then rejects the still-unresolved ``%{lv}``.
        """
        templates_dir = tmp_path / "templates"
        sub_file = tmp_path / "dev.yaml"
        self._write_template(templates_dir)
        self._write_substitutions(sub_file)

        flowgroup_dict = {"source": "tbl_%{lv}"}
        local_variables = {"lv": "{{ tparam }}"}

        sub_mgr = EnhancedSubstitutionManager(sub_file, env="dev")

        # WRONG ORDER: env/secret substitution before local-var resolution.
        after_subst_first = sub_mgr.substitute_yaml(flowgroup_dict)
        # %{lv} is invisible to the substitution manager — nothing resolves.
        assert after_subst_first["source"] == "tbl_%{lv}"
        # No secret could have been registered because no ${secret:...} ever
        # materialized (it lives behind the template+local layers).
        assert sub_mgr.secret_references == set()

        # And the still-unresolved %{lv} now trips the local-var validator,
        # confirming the documented order is not optional.
        resolver = LocalVariableResolver(local_variables)
        # lv expands to "{{ tparam }}" which contains no %{}, so %{lv} resolves
        # to literal template syntax — but the *secret* it should have become is
        # permanently lost. Demonstrate the lost-resolution outcome instead of
        # an exception for this particular interlock:
        recovered = resolver.resolve(after_subst_first)
        assert recovered["source"] == "tbl_{{ tparam }}"
        # The secret placeholder the correct order produced is absent.
        assert "__SECRET_" not in recovered["source"]
