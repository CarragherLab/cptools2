# cptools2 — Python package

A lightweight command-line package to generate and manage CellProfiler analysis jobs for HPC clusters. cptools2 builds image lists, splits work into jobs, creates space-optimized plate batches, generates submission scripts, and can join and optionally transfer result CSVs after analysis.

---

## Release highlights (v1.0.0)

- Packaging consolidated under `pyproject.toml` with modern metadata and direct references to supporting parser utilities.
- Scratch-space batching defaults increased to 75% utilisation with a 30% per-plate overhead buffer.
- Installation instructions updated for both `pip` and `uv` workflows.

## Table of contents

- [Release highlights (v1.0.0)](#release-highlights-v100)
- [Installation](#installation)
- [Quick usage](#quick-usage)
- [YAML configuration](#yaml-configuration)
- [Behavior notes](#behavior-notes)
- [Developer quickstart & testing](#developer-quickstart--testing)
- [HPC validation checklist](#hpc-validation-checklist)
- [Contributing](#contributing)
- [License](#license)

---

## Installation

### pip / virtualenv

```bash
python -m venv .venv
source .venv/bin/activate    # On Windows: .venv\Scripts\activate
pip install --upgrade pip
pip install -e .[dev]
```

Notes:
- Runtime dependencies (`pandas`, `pyyaml`, `parserix`, `scissorhands`) are resolved automatically via `pyproject.toml`.
- Use `pip install .` for a pure runtime install without developer extras.

### uv workflow

```bash
uv venv
uv pip install .[dev]  # or: uv pip install .
```

To run commands without activating the environment explicitly, prefix with `uv run`, e.g. `uv run pytest`.

## Quick usage

Generate a full workflow (creates loaddata, per-plate command files and a master submit script):

```bash
cptools2 generate config.yml
```

Join chunked CSV outputs after analysis (one or more patterns):

```bash
cptools2 join --location /path/to/location --patterns Image.csv Cells.csv
```

## YAML configuration

cptools2 accepts a YAML configuration file describing the experiment, pipeline, and optional features such as batching and transfer. A canonical example is included at `tests/new_config.yaml`.

Sanitized example (matches `tests/new_config.yaml`):

```yaml
chunk: 96
join_files:
  - Image.csv
location: /path/to/scratch/$USER/project/outputs
commands location: /path/to/scratch/$USER/project/commands
pipeline: /path/to/pipeline.cppipe

add plate:
  - experiment: /path/to/imagexpress/experiment
    plates:
      - plate_1
      - plate_2

data_destination: /path/to/datastore/project/data
```

Common fields:
- `experiment` / `add plate`: where to find image data and which plates to include
- `chunk`: desired images-per-job (integer)
- `pipeline`: path to the `.cppipe` CellProfiler pipeline
- `location`: base location for image outputs
- `commands location`: directory to write command files
- `join_files`: list of CSV filenames to join after analysis
- `data_destination`: optional path for post-join transfer

Advanced sections:
- `batching` — overrides for automatic batching (if supported)
- `transfer` — transfer provider configuration (S3 or other); cptools2 will write transfer metadata/commands but actual transfer depends on runner hooks

## Behavior notes

- `generate` will discover plates under the `experiment`, create image lists, split jobs according to `chunk`, apply batching overrides (if present), and write command files into `commands location`.
- `join` concatenates/join CSV outputs after the analysis; provide filename patterns to target.
- Transfer entries in the config are optional. `generate` records transfer commands/metadata; running transfers typically requires cluster-side hooks or CI steps that read the produced metadata.

## Developer quickstart & testing

Run tests:

```bash
uv run pytest  # or: pytest
```

Run linters/formatters (configured via pre-commit):

```bash
pre-commit run --all-files
```

## HPC validation checklist

These steps mirror the checks typically performed on the Eddie HPC cluster:

1. **Scratch quota assessment** – run `cptools2 generate` with real experiment configs to confirm 75% utilisation and overhead handling fits within assigned scratch space.
2. **Batch command generation** – verify per-batch command files (`staging_batch_*.txt`, `cp_commands_batch_*.txt`) are produced and reference the expected plates.
3. **CellProfiler dry run** – execute one batch via the HPC queue to confirm staging, CellProfiler invocation, and cleanup complete without exceeding scratch limits.
4. **Packaging install test** – from a clean node, run `pip install git+https://github.com/CarragherLab/cptools2@v1.0.0` (or sync via `uv`) to ensure dependencies resolve correctly.
5. **Post-analysis join** – validate `cptools2 join` against batch outputs for consistency with historical runs.

Document outcomes for each release to maintain an audit trail.

## Contributing

- Open issues or PRs describing bugs or enhancements.
- Keep changes small and focused; tests should accompany functional changes.

## License

This project is distributed under the MIT License. See `LICENSE`.


