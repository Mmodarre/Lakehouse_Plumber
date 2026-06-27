"""Unity Catalog tagging configuration."""

from pydantic import BaseModel


class UCTaggingConfig(BaseModel):
    """Project-level ``uc_tagging`` configuration from lhp.yaml.

    Gates the generated per-pipeline tagging event hook. ``enabled`` defaults to
    True, so an absent ``uc_tagging`` block behaves as enabled; auto-detection
    then only emits the hook when some write action or schema column declares
    ``tags``.

    ``remove_undeclared_tags`` selects the reconcile behavior: when False
    (default) tag setting is purely additive (create/update only); when True the
    hook also deletes any existing tag whose key is not in the declared set for a
    managed entity (reconcile to the declared state).
    """

    enabled: bool = True
    remove_undeclared_tags: bool = False
