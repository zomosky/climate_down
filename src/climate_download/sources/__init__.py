"""Source adapter package.

Importing this module triggers registration of every built-in source via
the :func:`register` decorator. External packages can register their own
adapters by importing ``climate_download.sources.registry.register`` and
applying it to their class — no edits to this file are required.
"""

from __future__ import annotations

from climate_download.sources.base import (
    BaseSource,
    Source,
    StepDownloadResult,
    VariableInfo,
)
from climate_download.sources.registry import (
    SOURCE_REGISTRY,
    get_source,
    list_sources,
    register,
)

# Side-effect imports: each module's @register call populates SOURCE_REGISTRY.
from climate_download.sources import aifs as _aifs  # noqa: F401
from climate_download.sources import gfs as _gfs  # noqa: F401
from climate_download.sources import hrrr as _hrrr  # noqa: F401
from climate_download.sources import icon as _icon  # noqa: F401

__all__ = [
    "BaseSource",
    "SOURCE_REGISTRY",
    "Source",
    "StepDownloadResult",
    "VariableInfo",
    "get_source",
    "list_sources",
    "register",
]
