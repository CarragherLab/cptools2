# cptools2 — Python package

A lightweight command-line package to generate and manage CellProfiler analysis jobs for HPC clusters. cptools2 builds image lists, splits work into jobs, creates space-optimized plate batches, generates submission scripts, and can join and optionally transfer result CSVs after analysis.

---

## Table of contents

- [Key features](#key-features)
- [Installation (developer)](#installation-developer)
- [Quick usage](#quick-usage)
- [YAML configuration](#yaml-configuration)
- [Behavior notes](#behavior-notes)
- [Developer quickstart & testing](#developer-quickstart--testing)
- [Contributing](#contributing)
- [License](#license)

---

## Key features

- Generate `loaddata` files and CellProfiler command lines from YAML configs
- Space-optimized plate batching for cluster submission
- Master submission scripts and per-batch command files
- Join chunked CSV result files after analysis (e.g., `Image.csv`, `Cells.csv`)
- Optional post-run transfer hooks (S3 or other datastores)


## Installation (developer)

Create a virtual environment and install the package with development extras:

```bash
python -m venv .venv
source .venv/bin/activate    # On Windows: .venv\Scripts\Activate.ps1
pip install -e .[dev]
pre-commit install
```

Notes:
- Root `pyproject.toml` is the single source of packaging metadata.
- Use `pip install -e .[dev]` to get the testing and linting tools.


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
new_ix: true
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
pytest
```

Run linters/formatters (configured via pre-commit):

```bash
pre-commit run --all-files
```


## Contributing

- Open issues or PRs describing bugs or enhancements.
- Keep changes small and focused; tests should accompany functional changes.


## License

This project is distributed under the MIT License. See `LICENSE`.


