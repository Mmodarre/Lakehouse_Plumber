"""Contract reference configuration for the ``contract`` action field.

An action carries a single ``contract`` field (this model) pointing at an ODCS
data-contract file plus options that tune what the contract-resolution pass
injects. What gets injected is implicit by action type (schema for cloudfiles
loads / writes / schema-transforms; expectations for data_quality transforms);
the options below only tune it.
"""

from typing import Any, Optional

from pydantic import BaseModel, Field


class ContractConfig(BaseModel):
    """The ``contract`` action field: file path + resolution options."""

    file: str = Field(..., description="Path to the contract file, relative to project root.")
    type: str = Field("odcs", description="Contract format; only 'odcs' is supported.")
    object: Optional[str] = Field(
        None,
        description=(
            "Schema object within the contract to use. Defaults to the contract's "
            "sole object; required when the contract has more than one object."
        ),
    )
    schema_hints: bool = Field(
        False,
        description=(
            "cloudfiles load only: when true, also emit cloudFiles.schemaHints "
            "(in addition to the default source.schema)."
        ),
    )
    expectations_action: Optional[str] = Field(
        None,
        description=(
            "data_quality only: 'warn'/'drop'/'fail' applied to all derived "
            "expectations; None defers to each property's criticalDataElement."
        ),
    )

    def model_post_init(self, __context: Any) -> None:
        """Normalize the file path for cross-platform compatibility."""
        if isinstance(self.file, str):
            self.file = self.file.replace("\\", "/")
