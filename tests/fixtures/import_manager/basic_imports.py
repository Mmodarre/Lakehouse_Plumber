"""Basic Python file with standard import patterns."""

import os
import sys
from pathlib import Path
from typing import Dict, List, Optional
import json
from datetime import datetime

def sample_function():
    """Sample function using imported modules."""
    current_path = Path.cwd()
    data = {"timestamp": datetime.now().isoformat()}
    return json.dumps(data) 