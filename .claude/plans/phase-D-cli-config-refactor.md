# Phase D: Parse YAML & CLI Refactor

## Objective

Refactor `parse_yaml.py` from namedtuple to dict-based config with params.json output for Nextflow, refactor `__main__.py` CLI to `pipeline`/`prepare`/`join` subcommands, and remove superseded modules from the ai-update branch.

## Scope

### Included:
- `parse_yaml.py`: replace namedtuple with dict + validation, add params.json generation, extend `check_yaml_args()` for new YAML keys (channels, stages, segmentation, feature_extraction)
- `__main__.py`: remove `generate` subcommand, add `pipeline` and `prepare` subcommands, keep `join`
- `__init__.py`: remove imports of superseded modules (generate_scripts, job, commands)
- Remove or mark superseded: `generate_scripts.py`, `job.py`, `commands.py` (keep files but remove from __init__.py imports)
- `pyproject.toml`: remove `scissorhands` from dependencies
- Example YAML config with new fields (channels, stages, segmentation, feature_extraction)
- Error handling: nextflow-not-found check, params.json validation
- `--dry-run` flag: generate params.json without invoking Nextflow
- `--resume` flag: skip data prep, pass `-resume` to Nextflow
- `--stages` flag with alias support (illum → illum_calculate + illum_apply)

### Explicitly NOT included:
- Polars migration (Lane A)
- Nextflow pipeline files (Lane B)
- Container builds (Lane C)
- The actual `nextflow run` invocation logic (just the CLI wrapper that calls it)

## Key Deliverables

| Deliverable | Format | Location |
|---|---|---|
| Refactored parse_yaml.py | Python | `cptools2/parse_yaml.py` |
| Refactored __main__.py | Python | `cptools2/__main__.py` |
| Updated __init__.py | Python | `cptools2/__init__.py` |
| Updated pyproject.toml | TOML | `pyproject.toml` |
| Example pipeline config | YAML | `tests/pipeline_config.yaml` |
| Updated test_parse_yaml.py | Python | `tests/test_parse_yaml.py` |
| New test_cli.py | Python | `tests/test_cli.py` |

## Success Criteria

- ✓ `parse_yaml.parse_config_file()` returns a plain dict (not namedtuple) — verified by `isinstance(result, dict)`
- ✓ `parse_yaml.generate_params_json(config_dict, output_path)` writes valid JSON that Nextflow can consume via `-params-file`
- ✓ `check_yaml_args()` accepts new keys: `channels`, `stages`, `segmentation`, `feature_extraction`, `containers`
- ✓ `cptools2 pipeline config.yml --dry-run` generates params.json without invoking Nextflow — verified by file existence and JSON validity
- ✓ `cptools2 pipeline config.yml --stages illum` resolves alias to `illum_calculate` + `illum_apply` in params.json
- ✓ `cptools2 prepare config.yml` generates LoadData CSVs and filelists without invoking Nextflow
- ✓ `cptools2 join --location /path --patterns Image.csv` still works (unchanged from master)
- ✓ `cptools2 generate` is removed — exits with "Use 'cptools2 pipeline' instead" message
- ✓ `import cptools2` does not import `generate_scripts`, `job`, or `commands`
- ✓ `scissorhands` removed from pyproject.toml dependencies
- ✓ All tests pass: `pytest tests/ -v` exits 0
- ✓ `cptools2 --version` still works
- ✓ `cptools2 pipeline config.yml` with nextflow not on PATH prints clear error with install instructions

## Dependencies

### Must Complete Before:
- None — CLI refactor can proceed independently

### Blocked By:
- Nothing

### Optional:
- Lane A (polars): parse_yaml calls loaddata/splitter which are being migrated. Dict config approach is compatible with either pandas or polars versions.
- Lane B (Nextflow): the `pipeline` subcommand invokes `nextflow run`, but the CLI wrapper can be written before main.nf exists (just needs to call the right command)

## Skills Required (Broad Categories)

- `python-testing`: CLI argument parsing tests, params.json generation tests
- `eddie-script-standards`: understanding what Nextflow params.json needs to contain

## Risk Assessment

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| Removing generate breaks existing user workflows | Low | Low | ai-update branch only. Master retains generate. Clear error message redirecting to pipeline. |
| params.json schema mismatch with Nextflow expectations | Medium | Medium | Validate against Nextflow docs; test with `-profile test` in Lane B integration |
| CLI argument conflicts between old and new subcommands | Low | Low | Clean break: remove generate entirely, no aliasing |
| scissorhands removal breaks import chain | Low | Low | Remove from __init__.py imports first, then from pyproject.toml |

## Assumptions

- `argparse sufficient for CLI`: no need for click or typer. argparse handles subcommands fine.
- `Nextflow params.json format is stable`: flat JSON dict with params as keys. Well-documented.
- `parse_yaml backward compat not needed on ai-update`: clean break. New YAML schema.

## Notes / Design Decisions

- **Worktree isolation**: this lane touches ONLY `cptools2/parse_yaml.py`, `cptools2/__main__.py`, `cptools2/__init__.py`, `pyproject.toml`, and their test files. No overlap with Lanes A, B, or C.
- **Dict over dataclass**: parse_config_file returns a plain dict. Simpler than dataclass, maps directly to JSON for params.json. Validation happens in check_yaml_args() and per-field validators.
- **generate subcommand removal**: prints a deprecation message pointing to `pipeline`. Does not silently break.
- **prepare subcommand**: runs steps 1-5 of the pipeline (parse, discover, LoadData, resolve containers, write params.json) without step 6 (invoking Nextflow). Useful for debugging and dry-run workflows.
- **Stage aliases**: `--stages illum` expands to `["illum_calculate", "illum_apply"]` in params.json. The expansion table is in the design doc (Stage Name Vocabulary section).

## Ralph Loops (3)

| Loop | Name | Type | Key Outputs |
|---|---|---|---|
| D10 | parse_yaml refactor: namedtuple → dict + params.json | Migration | Refactored parse_yaml.py, generate_params_json(), updated tests |
| D20 | CLI refactor: pipeline/prepare/join subcommands | Implementation | Refactored __main__.py, --dry-run, --resume, --stages flags, test_cli.py |
| D30 | Cleanup: remove superseded modules + scissorhands | Migration | Updated __init__.py, pyproject.toml, deprecation message for generate |
