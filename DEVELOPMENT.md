# Development - cptools2 package (repository root)

This document describes how to set up `cptools2` for local development and how the
repository is organized.

Install locally for development:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .[dev]
```

Run tests:

```bash
pytest
```

Package layout:

- `cptools2/` - package modules (CLI entrypoint in `__main__.py`)
- `tests/` - package tests

Versioning: bump `__version__` in `cptools2/__init__.py` when releasing.


