from typing import Any, Dict, List, Optional
from pydantic import BaseModel


class GenerateRequest(BaseModel):
    """Request body for POST /generate and POST /generate/preview."""
    environment: str
    pipeline: Optional[str] = None     # None = all pipelines
    force: bool = False
    include_tests: bool = False
    pipeline_config: Optional[str] = None  # Custom pipeline config path


class GenerateResponse(BaseModel):
    success: bool
    files_written: int
    total_flowgroups: int
    generated_files: Dict[str, str]    # filename → content (or path)
    output_location: Optional[str] = None
    error_message: Optional[str] = None
    performance_info: Dict[str, Any] = {}


class PreviewResponse(BaseModel):
    """Dry-run result: what WOULD be generated."""
    success: bool
    would_generate: Dict[str, str]     # filename → content preview
    total_flowgroups: int
    generation_mode: str               # "smart", "force", "selective"
