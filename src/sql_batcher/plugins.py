"""
Plugin system for SQL Batcher.

This module provides the plugin system for SQL Batcher.
"""

from enum import Enum
from typing import Any, Dict, List, Optional

class HookType(Enum):
    """Hook types for plugins."""

    PRE_BATCH = "pre_batch"
    POST_BATCH = "post_batch"
    ON_ERROR = "on_error"
