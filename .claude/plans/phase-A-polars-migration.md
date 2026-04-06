# Phase A: Polars Migration

## Objective

Migrate all internal DataFrame operations from pandas to polars across the cptools2 Python companion library, improving performance for large-scale CSV joining and LoadData generation.

## Scope

### Included:
- `loaddata.py`: `pd.DataFrame` → `pl.DataFrame`, `pivot_table()` → `pl.DataFrame.pivot()`, remove inplace operations
- `splitter.py`: `pd.DataFrame` → `pl.DataFrame`, groupby iteration → `partition_by()` or `group_by().agg()`
- `file_tools.py`: `pd.read_csv` → `pl.read_csv`, `pd.concat` → `pl.concat`, `.merge()` → `.join()`
- `utils.py`: prune orchestration helpers (make_dir, prefix_filepaths, sanitise_filename, make_executable, count_lines_in_file), adapt remaining functions (`any_nan_values`, `flatten`) for polars
- `pyproject.toml`: replace `pandas>=1.3` with `polars>=1.0` in dependencies
- All tests updated for polars DataFrame assertions
- New: `splitter.split_by_plate()` function for per-plate partitioning (needed by Nextflow illum_calculate process)
- New: `loaddata.create_loaddata()` `illum_dir` parameter for illumination function columns
- New: `containers.is_gpu_container()` for manifest GPU flag lookup

### Explicitly NOT included:
- Nextflow pipeline files (Lane B)
- Container building/deployment (Lane C)
- CLI or parse_yaml changes (Lane D)
- CellProfiler .cppipe templates (Lane B)

## Key Deliverables

| Deliverable | Format | Location |
|---|---|---|
| Updated loaddata.py | Python | `cptools2/loaddata.py` |
| Updated splitter.py | Python | `cptools2/splitter.py` |
| Updated file_tools.py | Python | `cptools2/file_tools.py` |
| Updated utils.py (pruned) | Python | `cptools2/utils.py` |
| Updated containers.py | Python | `cptools2/containers.py` |
| Updated pyproject.toml | TOML | `pyproject.toml` |
| Updated test_loaddata.py | Python | `tests/test_loaddata.py` |
| Updated test_splitter.py | Python | `tests/test_splitter.py` |
| Updated test_file_tools.py | Python | `tests/test_file_tools.py` |
| Updated test_utils.py | Python | `tests/test_utils.py` |
| Updated test_containers.py | Python | `tests/test_containers.py` |

## Success Criteria

- ✓ No `import pandas` in any file under `cptools2/` — verified by `grep -r "import pandas" cptools2/`
- ✓ `polars>=1.0` in pyproject.toml dependencies, `pandas` removed
- ✓ `loaddata.create_loaddata()` returns a `polars.DataFrame` with identical column names and values to previous pandas output — verified by CSV comparison against fixture
- ✓ `loaddata.create_loaddata(img_list, illum_dir="/path")` produces DataFrame with `FileName_Illum_W1..W5` and `PathName_Illum_W1..W5` columns
- ✓ `splitter.split()` produces identical chunked output — verified by existing test assertions
- ✓ `splitter.split_by_plate(plate_store)` returns one image list per plate with all images in a single partition
- ✓ `file_tools.merge_loaddata_metadata()` works correctly with polars — verified by test_file_tools.py
- ✓ `containers.is_gpu_container(container_dir, role)` reads manifest `gpu` field
- ✓ `utils.py` contains only `flatten()` and `any_nan_values()` — no orchestration helpers
- ✓ All tests pass: `pytest tests/ -v` exits 0
- ✓ Test count >= 84 (current baseline)

## Dependencies

### Must Complete Before:
- None — this lane has no dependencies on other lanes

### Blocked By:
- Nothing

### Optional:
- Lane D (parse_yaml refactor): loaddata and splitter produce DataFrames consumed by parse_yaml's params.json generation, but the interface is stable (function signatures unchanged)

## Skills Required (Broad Categories)

- `polars`: DataFrame operations, pivot, join, lazy evaluation patterns
- `python-testing`: updating test assertions for polars API differences

## Risk Assessment

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| Polars pivot API differs from pandas pivot_table | Medium | Medium | Test with fixture data; polars `pivot()` supports `aggregate_function="first"` |
| Groupby iteration pattern change breaks splitter logic | Medium | Medium | Use `partition_by()` and verify output matches existing test expectations |
| Polars lazy evaluation causes unexpected behavior in tests | Low | Low | Use eager mode (`.collect()`) in tests; lazy mode for production performance |
| parserix returns Python lists, not polars Series | Low | Low | Convert at boundary: `pl.Series(parserix_result)` |

## Assumptions

- `Polars API stable at 1.0+`: the pivot, join, and concat APIs are stable post-1.0
- `CSV output identical`: polars `write_csv()` produces byte-identical output to pandas `to_csv(index=False)` for the column types used (strings, ints)
- `parserix compatibility`: parserix returns plain Python lists/strings, no pandas dependency

## Notes / Design Decisions

- **Worktree isolation**: this lane touches ONLY `cptools2/loaddata.py`, `cptools2/splitter.py`, `cptools2/file_tools.py`, `cptools2/utils.py`, `cptools2/containers.py`, `pyproject.toml`, and their test files. No overlap with Lanes B, C, or D.
- **New functionality bundled**: `split_by_plate()`, `illum_dir` parameter, and `is_gpu_container()` are included here because they're small additions to files already being modified for polars. Avoids touching these files twice.
- **utils.py pruning**: orchestration helpers are removed because they're only used by `generate_scripts.py`, `job.py`, and `commands.py` — all superseded by Nextflow. Keep `flatten()` (used in splitter/job) and `any_nan_values()` (used in loaddata).

## Ralph Loops (4)

| Loop | Name | Type | Key Outputs |
|---|---|---|---|
| A10 | Polars migration: loaddata.py | Migration | Updated loaddata.py with polars, illum_dir param, updated tests |
| A20 | Polars migration: splitter.py + split_by_plate | Migration | Updated splitter.py with polars, split_by_plate(), updated tests |
| A30 | Polars migration: file_tools.py + utils.py pruning | Migration | Updated file_tools.py, pruned utils.py, updated tests |
| A40 | Containers extension + pyproject.toml + final validation | Implementation | is_gpu_container(), polars dependency, full test suite green |
