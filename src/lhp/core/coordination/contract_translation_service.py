"""Pre-generation step: translate ODCS contracts into LHP ``contracts/lhp/`` artifacts.

Runs before discovery/validation/generation (invoked from the facade
composition root). Slice 1 emits only schema files under ``contracts/lhp/schemas/``.
Output lives beside the source contracts (mirroring how generated Databricks
Asset Bundle resources are stored under ``resources/lhp/``) and is intended to be
version-controlled, not gitignored. The step is opt-in by folder presence: a
no-op when ``contracts/`` is absent.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional

import yaml


class _IndentedSafeDumper(yaml.SafeDumper):
    """SafeDumper that indents block sequences under their parent key.

    PyYAML's default renders list items flush with their key
    (``columns:\\n- name: ...``); LHP convention indents them
    (``columns:\\n  - name: ...``, as in hand-authored ``contracts/*.yaml``).
    Forcing ``indentless=False`` produces the indented style.
    """

    def increase_indent(self, flow: bool = False, indentless: bool = False):
        return super().increase_indent(flow, indentless=False)


@dataclass
class TranslationResult:
    """Outcome of a contract-translation pass.

    :param schema_files: absolute paths written under ``contracts/lhp/schemas/``.
    :param contracts_processed: number of contract files parsed.
    """

    schema_files: List[Path] = field(default_factory=list)
    contracts_processed: int = 0


class ContractTranslationService:
    """Translate ``contracts/*.yaml`` ODCS files into ``contracts/lhp/schemas/`` files."""

    def __init__(
        self, project_root: Path, logger: Optional[logging.Logger] = None
    ) -> None:
        self.project_root = Path(project_root)
        self.logger = logger or logging.getLogger(__name__)
        self.contracts_dir = self.project_root / "contracts"
        self.schemas_out_dir = self.project_root / "contracts" / "lhp" / "schemas"

    def translate(self) -> TranslationResult:
        """Parse every ODCS contract and write equivalent LHP schema files.

        No-op (empty result) when ``contracts/`` does not exist. Otherwise, for
        each ``contracts/*.{yaml,yml}`` (sorted): parse + validate, translate
        each schema object, and write
        ``contracts/lhp/schemas/<stem>.<object>_schema.yaml`` via
        :func:`lhp.utils.file_header.write_normalized`. The output is a
        deterministic function of the input, so re-running on unchanged contracts
        rewrites byte-identical files (a harmless no-op diff).

        :raises lhp.errors.LHPError: ``LHP-CFG-062`` / ``LHP-CFG-063`` on invalid
            contracts or unmappable types.
        """
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
                content = yaml.dump(
                    artifact.schema_dict,
                    Dumper=_IndentedSafeDumper,
                    default_flow_style=False,
                    sort_keys=False,
                    indent=2,
                )
                write_normalized(out_path, content)
                result.schema_files.append(out_path)

        return result
