# Phase 1: Container Integration & Polars Migration

## Objective

Replace all conda-based CellProfiler execution with Singularity container invocations, and migrate internal DataFrame operations from pandas to polars for better performance on large single-cell datasets.

## Scope

### Included:

**Container integration:**
- `generate_scripts.py`: Replace `load_module_text()` — emit `module load singularity` only
- `commands.py`: Wrap CellProfiler commands in `singularity exec`; add generic `container_command()`
- `containers.py`: Allow unknown roles from manifest; add GPU flag lookup; remove/demote `DEFAULT_CONTAINERS`
- `parse_yaml.py`: Make `container_path` mandatory; expand config for future pipeline stages
- `__main__.py`: Pass container path through `handle_generate()` to script generation
- Analysis script generation: Switch from scissorhands `AnalysisScript` to `SafePathScript` (avoid conda activation)
- Memory specification: Ensure all generated scripts use `h_rss` (not `h_vmem`)

**Polars migration (pandas → polars in all production code):**
- `file_tools.py`: `pd.read_csv` → `pl.read_csv`, `pd.concat` → `pl.concat`, `.merge()` → `.join()`
- `loaddata.py`: `pd.DataFrame` → `pl.DataFrame`, `pivot_table()` → `pl.DataFrame.pivot()`, remove inplace operations
- `splitter.py`: `pd.DataFrame` → `pl.DataFrame`, `groupby` iteration → `partition_by()` or `group_by().agg()`
- `utils.py`: DataFrame column operations and null checks adapted to polars API
- `commands.py`: Docstring type reference update only
- `pyproject.toml`: Replace `pandas>=1.3` with `polars>=1.0` in dependencies
- Update all tests for polars DataFrame assertions

### Explicitly NOT included:
- Building or deploying containers (Phase 0)
- New pipeline stages — illumination, segmentation, feature extraction (Phases 2-4)
- `cptools2 pipeline` orchestrator command (Phase 5)
- New CLI subcommands (`illum`, `segment`, `extract`)
- Database output format (each tool uses its native default: CSV for CellProfiler, .npz for DeepProfiler)

## Key Deliverables

| Deliverable | Format | Location |
|---|---|---|
| Updated generate_scripts.py | Python | `cptools2/generate_scripts.py` |
| Updated commands.py | Python | `cptools2/commands.py` |
| Updated containers.py | Python | `cptools2/containers.py` |
| Updated parse_yaml.py | Python | `cptools2/parse_yaml.py` |
| Updated __main__.py | Python | `cptools2/__main__.py` |
| Updated file_tools.py (polars) | Python | `cptools2/file_tools.py` |
| Updated loaddata.py (polars) | Python | `cptools2/loaddata.py` |
| Updated splitter.py (polars) | Python | `cptools2/splitter.py` |
| Updated utils.py (polars) | Python | `cptools2/utils.py` |
| Updated pyproject.toml | TOML | `pyproject.toml` |
| Updated test suite | Python | `tests/` |

## Success Criteria

- ✓ `cptools2 generate config.yml` produces SGE scripts containing `module load singularity` — verified by grep on generated .sh files
- ✓ No generated script contains `module load anaconda`, `conda activate`, or any conda reference — verified by grep
- ✓ All CellProfiler commands in generated `cp_commands_batch_*.txt` files decode (base64) to `singularity exec --bind ... cellprofiler ...` — verified by decoding and inspecting
- ✓ `generate_scripts.load_module_text()` returns only `module load singularity\n` with no parameters
- ✓ `commands.cp_command()` accepts `container_path` and `gpu` parameters and wraps command in `singularity exec`
- ✓ `commands.container_command()` exists as a generic wrapper for non-CellProfiler tools
- ✓ `containers.py` resolves arbitrary roles from manifest (not just hardcoded defaults) — verified by test with a custom role name
- ✓ `containers.is_gpu_container(role)` reads manifest `gpu` field correctly
- ✓ `parse_yaml.py` raises an error if `container_path` is missing and `CPTOOLS2_CONTAINER_DIR` is not set
- ✓ All existing tests pass (updated for singularity expectations)
- ✓ `_create_analysis_script()` uses `SafePathScript` (not scissorhands `AnalysisScript`) to avoid conda activation logic
- ✓ All memory specifications in generated scripts use `h_rss` (not `h_vmem`)
- ✓ No `import pandas` in any production code file — verified by grep across `cptools2/`
- ✓ `polars` declared as dependency in `pyproject.toml`, `pandas` removed from required dependencies
- ✓ `loaddata.create_loaddata()` returns a `polars.DataFrame` with identical column names and values to the previous pandas output — verified by comparing CSV output against fixture
- ✓ `splitter.split()` produces identical chunked output with polars internals — verified by existing test assertions
- ✓ `file_tools.merge_loaddata_metadata()` works with polars DataFrames — verified by test_file_tools.py (already uses polars-style assertions)
- ✓ `loaddata.cast_dataframe()` pivot produces correct wide format — verified by test_loaddata.py

## Dependencies

### Must Complete Before This Phase:
- None — code changes can proceed independently of container deployment (Phase 0)

### Blocked By:
- Nothing — this phase modifies Python code only; no Eddie access required

### Optional:
- Phase 0 containers: Having `.sif` files on Eddie enables end-to-end validation but is not required for code changes and local tests

## Skills Required (Broad Categories)

- `python-testing`: Updating test fixtures and assertions for singularity output
- `eddie-script-standards`: Ensuring generated SGE scripts follow Eddie conventions (h_rss, hold_jid, PE syntax)

## Risk Assessment

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| scissorhands AnalysisScript removal breaks other functionality | Medium | Medium | Audit all uses of AnalysisScript; SafePathScript already handles staging/destaging successfully |
| Bind path assumptions don't cover all Eddie use cases | Low | Medium | Default to `/exports/eddie/scratch/$USER`; make bind_paths configurable in YAML |
| Tests too tightly coupled to conda output format | Medium | Low | Update test assertions systematically; search for all "anaconda" and "conda" strings in tests |
| container_path mandatory breaks users without containers | Low | Low | This is the ai-update branch — master retains conda. No backward compat needed. |

## Assumptions

- `SafePathScript sufficient`: The existing `SafePathScript` class (already used for staging/destaging) can replace `AnalysisScript` for CellProfiler jobs — validated by inspecting that it supports all needed SGE directives
- `scissorhands still needed`: scissorhands `script_generator.SGEScript` base class is still used for script structure — only `AnalysisScript` (conda-specific) is being replaced
- `Base64 encoding preserved`: The existing base64-encoded command pattern works identically with singularity-wrapped commands — the decode-and-execute pattern is shell-agnostic

## Notes / Design Decisions

- **No dual-path code**: This branch fully commits to containers. No `if container_path: singularity else: conda` branches. The conda path is preserved on `master`.
- **`container_command()` is generic**: It wraps any command in `singularity exec`, not just CellProfiler. This is the extension point for DeepProfiler, Cellpose, DINOv2, etc.
- **GPU flag from manifest, not hardcoded**: `--nv` is added when `containers.is_gpu_container(role)` returns True, which reads the manifest's `gpu` field. No hardcoded GPU role list.
- **Memory: always h_rss**: Every generated script uses `-l h_rss=` per Eddie's September 2025 change. The `memory` parameter in SafePathScript maps to `h_rss`.

## Source File Change Summary

### `generate_scripts.py`
- `load_module_text()`: Remove `is_cellprofiler` param. Return `module load singularity\n` only.
- `_create_analysis_script()`: Replace `script_generator.AnalysisScript` with `SafePathScript`. Remove `load_module_text(is_cellprofiler=True)` call — just `load_module_text()`.
- `make_join_files_script()`: Update `load_module_text(is_cellprofiler=False)` call to `load_module_text()`.

### `commands.py`
- `cp_command()`: Add `container_path`, `gpu=False`, `bind_paths=None` params. Wrap in `singularity exec`.
- `make_cp_cmnd()`: Pass `container_path` through to `cp_command()`.
- New `container_command()`: Generic singularity exec wrapper.

### `containers.py`
- `_image_name_for_role()`: Accept any role found in manifest, not just `DEFAULT_CONTAINERS` keys.
- New `is_gpu_container(container_dir, role)`: Read manifest `gpu` field for a role.
- `DEFAULT_CONTAINERS`: Demote to fallback-only (manifest is authoritative).

### `parse_yaml.py`
- `container_path()`: Now mandatory — raise if not resolvable.
- `check_yaml_args()`: Add `containers`, `pipeline_stages`, `feature_extraction`, `segmentation`, `illumination` to valid args.
- `parse_config_file()`: Pass `container_path` through to config namedtuple (already partially done).

### `__main__.py`
- `handle_generate()`: Validate container setup early. Pass `container_path` to `jobber.create_commands()` and through to script generation.

### `loaddata.py` (polars migration)
- `import pandas as _pd` → `import polars as pl`
- `_pd.DataFrame()` → `pl.DataFrame()`
- `pivot_table(index=..., columns=..., values=..., aggfunc="first").reset_index()` → `pl.DataFrame.pivot(on=..., index=..., values=..., aggregate_function="first")`
- Remove `inplace=True` from `.rename()` and `.drop()` — polars returns new objects
- `.shape[0]` → `.height`

### `splitter.py` (polars migration)
- `import pandas as _pd` → `import polars as pl`
- `_pd.DataFrame()` → `pl.DataFrame()`
- `df.groupby([cols])` iteration → `df.partition_by([cols])` or `group_by().agg()` + iterate over groups
- `group["img_paths"]` → `group.get_column("img_paths").to_list()`

### `utils.py` (polars migration)
- DataFrame column operations: `.columns` works the same
- `.map(lambda)` → `.map_elements()` or expression-based approach
- `.isnull().any().any()` → `.null_count() > 0` or `pl.any_horizontal(pl.all().is_null())`

### `file_tools.py` (polars migration)
- `pd.read_csv()` → `pl.read_csv()`
- `pd.concat()` → `pl.concat()`
- `.merge(on=..., how="left")` → `.join(on=..., how="left")`
- `.isna().sum()` → `.null_count()`
- `.to_csv()` → `.write_csv()`

### `pyproject.toml`
- Replace `pandas>=1.3` with `polars>=1.0` in `[project.dependencies]`

## Ralph Loops (6)

| Loop | Name | Type | Key Outputs |
|---|---|---|---|
| 110 | Replace conda with singularity in script generation | Migration | Updated `generate_scripts.py`, `load_module_text()` simplified |
| 120 | Container-wrapped command generation | Implementation | Updated `commands.py` with `cp_command()` wrapping and `container_command()` |
| 130 | Container resolution and manifest extensibility | Implementation | Updated `containers.py` with arbitrary role support and GPU flag lookup |
| 140 | Config parsing and CLI wiring | Implementation | Updated `parse_yaml.py`, `__main__.py`, container path passed through |
| 150 | Polars migration — core modules | Migration | Updated `loaddata.py`, `splitter.py`, `utils.py` from pandas to polars |
| 160 | Polars migration — file_tools, pyproject.toml, and test updates | Migration | Updated `file_tools.py`, `pyproject.toml` deps, all 84+ tests passing |
