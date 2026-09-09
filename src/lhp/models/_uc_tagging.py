"""Unity Catalog tagging configuration."""

from typing import Optional

from pydantic import BaseModel, Field


class UCTaggingConfig(BaseModel):
    """Project-level ``uc_tagging`` configuration from lhp.yaml.

    The feature is **on by default**: an absent ``uc_tagging`` block behaves as
    these defaults (enabled). Users opt IN simply by declaring ``tags`` on a table
    or column — the hook is emitted only when some write action or schema column
    declares ``tags`` (auto-detection). To disable the feature, set
    ``uc_tagging.enabled: false`` in lhp.yaml.

    ``remove_undeclared_tags`` selects the reconcile behavior: when False (default)
    tag setting is purely additive (create/update only); when True the hook also
    deletes any existing tag whose key is not in the declared set for a managed
    entity (reconcile to the declared state).

    ``tag_update_concurrency`` is the max concurrent tag operations (the hook's
    ThreadPoolExecutor ``max_workers`` for the one-shot pass over all tables/columns);
    defaults to 16.

    ``max_allowable_consecutive_failures`` is passed straight through to the generated
    hook's ``@dp.on_event_hook``. Per the SDP contract it must be an integer >= 0 or
    ``None``; ``None`` (the default, matching SDP) means there is no limit to the
    consecutive failures allowed and the hook is never disabled. An integer caps the
    budget: SDP disables the hook after that many consecutive failures.
    """

    enabled: bool = True
    remove_undeclared_tags: bool = False
    tag_update_concurrency: int = Field(16, ge=1, le=20)
    max_allowable_consecutive_failures: Optional[int] = Field(None, ge=0)
