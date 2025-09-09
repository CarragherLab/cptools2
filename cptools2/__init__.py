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

__all__ = [
    "filelist",
    "splitter",
    "commands",
    "parse_yaml",
    "utils",
    "job",
    "colours",
    "file_tools",
]

__version__ = "0.0.0"
