"""cptools2 package initializer.

Expose top-level modules and package metadata.
"""

from . import filelist  # noqa: F401
from . import splitter  # noqa: F401
from . import commands  # noqa: F401
from . import parse_yaml  # noqa: F401
from . import utils  # noqa: F401
from . import job  # noqa: F401
from . import colours  # noqa: F401
from . import file_tools  # noqa: F401
from . import containers  # noqa: F401

try:
    from importlib import metadata as _metadata  # Python 3.8+
except ImportError:  # pragma: no cover
    import importlib_metadata as _metadata  # type: ignore

__all__ = [
    "filelist",
    "splitter",
    "commands",
    "parse_yaml",
    "utils",
    "job",
    "colours",
    "file_tools",
    "containers",
]

__version__ = _metadata.version("cptools2")
