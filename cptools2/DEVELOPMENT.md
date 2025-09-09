# Development - cptools2 package

This document describes how to set up `cptools2` for local development and how the
package is organized.

Install locally for development:

```bash
cd cptools2
python -m venv .venv
source .venv/bin/activate
pip install -e .
```

Run tests:

```bash
pytest
```

Package layout:

- `cptools2/` - package modules (CLI entrypoint in `__main__.py`)
- `tests/` - package tests

Versioning: bump `__version__` in `cptools2/__init__.py` when releasing.


