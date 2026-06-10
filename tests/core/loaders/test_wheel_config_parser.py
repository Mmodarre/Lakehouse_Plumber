"""Tests for ``parse_wheel_config`` (the ``wheel`` section of lhp.yaml).

All fields are optional; ``artifact_volume`` (if present) must be a string.
A non-mapping ``wheel`` value, or a non-string ``artifact_volume``, raises
``LHPError`` with code ``LHP-CFG-060``. The ``/Volumes/...`` shape is validated
later, post-substitution, and is intentionally NOT checked here.
"""

import pytest

from lhp.core.loaders._wheel_config_parser import parse_wheel_config
from lhp.errors import LHPError
from lhp.models import WheelConfig


@pytest.mark.unit
class TestParseWheelConfigValid:
    """Well-formed ``wheel`` mappings parse into a ``WheelConfig``."""

    def test_valid_mapping_returns_artifact_volume(self):
        """A mapping with a string artifact_volume round-trips onto the model."""
        config = parse_wheel_config({"artifact_volume": "/Volumes/cat/sch/vol"})

        assert isinstance(config, WheelConfig)
        assert config.artifact_volume == "/Volumes/cat/sch/vol"

    def test_empty_mapping_tolerated_artifact_volume_none(self):
        """An empty mapping is tolerated; artifact_volume defaults to None."""
        config = parse_wheel_config({})

        assert isinstance(config, WheelConfig)
        assert config.artifact_volume is None


@pytest.mark.unit
class TestParseWheelConfigInvalid:
    """Type violations raise ``LHPError`` with code ``LHP-CFG-060``."""

    @pytest.mark.parametrize(
        "bad_value",
        [
            "/Volumes/cat/sch/vol",  # string, not a mapping
            ["artifact_volume"],  # list, not a mapping
            123,  # int, not a mapping
        ],
        ids=["string", "list", "int"],
    )
    def test_non_mapping_raises_cfg_060(self, bad_value):
        """A non-mapping wheel value raises LHPError with code LHP-CFG-060."""
        with pytest.raises(LHPError) as exc_info:
            parse_wheel_config(bad_value)

        assert exc_info.value.code == "LHP-CFG-060"

    def test_wrong_type_artifact_volume_raises_cfg_060(self):
        """A non-string artifact_volume raises LHPError with code LHP-CFG-060."""
        with pytest.raises(LHPError) as exc_info:
            parse_wheel_config({"artifact_volume": 123})

        assert exc_info.value.code == "LHP-CFG-060"
