"""Pre-generation step: translate ODCS contracts into LHP ``.lhp/`` artifacts.

Runs before discovery/validation/generation (invoked from the facade
composition root). Slice 1 emits only schema files under ``.lhp/contracts/schemas/``.
The step is opt-in by folder presence: a no-op when ``contracts/`` is absent.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional


@dataclass
class TranslationResult:
    """Outcome of a contract-translation pass.

    :param schema_files: absolute paths written under ``.lhp/contracts/schemas/``.
    :param contracts_processed: number of contract files parsed.
    """

    schema_files: List[Path] = field(default_factory=list)
    contracts_processed: int = 0


class ContractTranslationService:
    """Translate ``contracts/*.yaml`` ODCS files into ``.lhp/contracts/schemas/`` files."""

    def __init__(
        self, project_root: Path, logger: Optional[logging.Logger] = None
    ) -> None:
        self.project_root = Path(project_root)
        self.logger = logger or logging.getLogger(__name__)
        self.contracts_dir = self.project_root / "contracts"
        self.schemas_out_dir = self.project_root / ".lhp" / "contracts" / "schemas"

    def translate(self) -> TranslationResult:
        """Parse every ODCS contract and write equivalent LHP schema files.

        No-op (empty result) when ``contracts/`` does not exist. Otherwise, for
        each ``contracts/*.{yaml,yml}`` (sorted): parse + validate, translate
        each schema object, and write ``.lhp/contracts/schemas/<stem>.<object>_schema.yaml`` via
        :func:`lhp.utils.file_header.write_normalized`. The output is a
        deterministic function of the input, so re-running on unchanged contracts
        rewrites byte-identical files (the ``.lhp/contracts/schemas/`` tree is gitignored
        and not state-tracked, so the rewrite is harmless).

        :raises lhp.errors.LHPError: ``LHP-CFG-062`` / ``LHP-CFG-063`` on invalid
            contracts or unmappable types.
        """
        import yaml

        from ...parsers.odcs_parser import OdcsParser
        from ...utils.file_header import write_normalized
        from ..processing.odcs_translator import OdcsTranslator

        result = TranslationResult()

        if not self.contracts_dir.exists():
            return result

        contract_files = sorted(
            p
            for p in self.contracts_dir.iterdir()
            if p.is_file() and p.suffix in (".yaml", ".yml")
        )
        if not contract_files:
            return result

        parser = OdcsParser()
        translator = OdcsTranslator()

        self.schemas_out_dir.mkdir(parents=True, exist_ok=True)

        for contract_file in contract_files:
            contract = parser.parse(contract_file)
            artifacts = translator.translate_schemas(
                contract, contract_stem=contract_file.stem
            )
            result.contracts_processed += 1

            for artifact in artifacts:
                out_path = self.schemas_out_dir / artifact.file_name
                content = yaml.safe_dump(artifact.schema_dict, sort_keys=False)
                write_normalized(out_path, content)
                result.schema_files.append(out_path)

        return result
