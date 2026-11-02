"""Streaming-table source validation for write actions.

A module-level function split out of :class:`WriteActionValidator` to keep
that class small (constitution §3.3). The per-mode checks stay on the class.
"""

from typing import List

from lhp.models import Action


def validate_streaming_table(action: Action, prefix: str) -> List[str]:
    errors = []

    # snapshot_cdc mode defines its source differently, so skip the check.
    mode = action.write_target.get("mode", "standard")
    if mode != "snapshot_cdc":
        if not action.source:
            errors.append(f"{prefix}: Streaming table must have 'source' to read from")
        elif not isinstance(action.source, (str, list)):
            errors.append(
                f"{prefix}: Streaming table source must be a string or list of view names"
            )
        elif (
            mode == "cdc" and isinstance(action.source, list) and len(action.source) > 1
        ):
            errors.append(
                f"{prefix}: CDC mode does not support multiple source views "
                f"in a single action. Define one write action per source, "
                f"each with compatible cdc_config, targeting the same "
                f"catalog.schema.table."
            )
        elif (
            mode == "replace"
            and isinstance(action.source, list)
            and len(action.source) > 1
        ):
            errors.append(
                f"{prefix}: replace mode does not support multiple source views "
                f"in a single action. A REPLACE USING target must be served by a "
                f"single flow reading one streaming source."
            )

    # REPLACE USING requires a streaming source (spark.readStream); a batch
    # read is rejected by the Databricks runtime.
    if mode == "replace" and action.readMode == "batch":
        errors.append(
            f"{prefix}: replace mode requires a streaming source; readMode "
            f"'batch' is not supported (REPLACE USING reads via "
            f"spark.readStream)."
        )

    return errors
