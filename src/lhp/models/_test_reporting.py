"""Test-result reporting configuration."""

from typing import Optional

from pydantic import BaseModel


class TestReportingConfig(BaseModel):
    __test__ = False  # Tell pytest this is not a test class

    module_path: str
    function_name: str
    config_file: Optional[str] = None
