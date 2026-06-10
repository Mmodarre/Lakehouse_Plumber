import logging
from abc import abstractmethod
from typing import Any, Dict, List

from lhp.models import Action

from ....core.registry import BaseActionGenerator

logger = logging.getLogger(__name__)


class BaseSinkWriteGenerator(BaseActionGenerator):
    def __init__(self):
        super().__init__(use_import_manager=True)
        self.add_import("from pyspark import pipelines as dp")
        self.add_import("from pyspark.sql import functions as F")

    @abstractmethod
    def generate(self, action: Action, context: Dict[str, Any]) -> str: ...

    def _extract_source_views(self, source) -> List[str]:
        if isinstance(source, str):
            return [source]
        if isinstance(source, list):
            views = []
            for item in source:
                if isinstance(item, str):
                    views.append(item)
                elif isinstance(item, dict) and "view" in item:
                    views.append(item["view"])
            return views
        if isinstance(source, dict):
            if "view" in source:
                return [source["view"]]
        return []
