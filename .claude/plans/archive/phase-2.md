# Phase 2: Illumination Correction Stages

## Objective

Add illumination correction as a two-stage pipeline (calculate + apply) to cptools2, including per-plate partitioning, LoadData CSV extension for illumination function columns, CellProfiler pipeline templates, and a new `cptools2 illum` CLI subcommand.

## Scope

### Included:
- `splitter.py`: New `split_by_plate()` function for per-plate partitioning
- `loaddata.py`: Extended `create_loaddata()` with `illum_dir` parameter for `.npy` columns
- Stage module: `cptools2/stages/base.py` — base class defining the stage interface
- Stage module: `cptools2/stages/illum_calculate.py` — Stage 1 (calculate illumination functions)
- Stage module: `cptools2/stages/illum_apply.py` — Stage 2 (apply correction, output 16-bit PNG)
- CellProfiler pipeline templates: `illum_calculate.cppipe`, `illum_apply.cppipe`
- CLI: `cptools2 illum calculate`, `cptools2 illum apply`, `cptools2 illum validate`
- SGE script generation for both stages with correct memory/dependency configuration
- Post-stage validation (`.npy` file count, corrected image count)

### Explicitly NOT included:
- Nuclear segmentation pipeline (Phase 3)
- Feature extraction (Phase 4)
- Pipeline orchestrator (Phase 5)
- Container building or deployment (Phase 0)

## Key Deliverables

| Deliverable | Format | Location |
|---|---|---|
| Stage base class | Python | `cptools2/stages/base.py` |
| Illum calculate stage | Python | `cptools2/stages/illum_calculate.py` |
| Illum apply stage | Python | `cptools2/stages/illum_apply.py` |
| Stage validation | Python | `cptools2/stages/validate.py` |
| Stages __init__ | Python | `cptools2/stages/__init__.py` |
| Illum calculate pipeline | CellProfiler .cppipe | `cptools2/templates/illum_calculate.cppipe` |
| Illum apply pipeline | CellProfiler .cppipe | `cptools2/templates/illum_apply.cppipe` |
| Updated splitter.py | Python | `cptools2/splitter.py` |
| Updated loaddata.py | Python | `cptools2/loaddata.py` |
| Updated __main__.py | Python | `cptools2/__main__.py` |
| Illum correction tests | Python | `tests/test_illum_*.py` |

## Success Criteria

- ✓ `splitter.split_by_plate(plate_store)` returns one image list per plate, with all images for each plate in a single partition — verified by unit test with fixture data
- ✓ Existing `splitter.split()` behaviour unchanged — verified by existing tests still passing
- ✓ `loaddata.create_loaddata(img_list, illum_dir="/path")` produces a DataFrame with `FileName_Illum_W1..W5` and `PathName_Illum_W1..W5` columns — verified by unit test
- ✓ `loaddata.create_loaddata(img_list)` (no `illum_dir`) produces identical output to current behaviour — verified by existing tests
- ✓ `illum_calculate.cppipe` contains CorrectIlluminationCalculate modules for 5 channels (DNA, RNA, ER, AGP, Mito) with "All" mode and median filter — verified by inspection
- ✓ `illum_apply.cppipe` contains CorrectIlluminationApply modules for 5 channels with division method and 16-bit PNG SaveImages — verified by inspection
- ✓ `cptools2 illum calculate config.yml` generates SGE scripts with `-pe sharedmem 4 -l h_rss=16G` and `singularity exec` — verified by grep on generated .sh
- ✓ `cptools2 illum apply config.yml` generates SGE scripts with `-hold_jid` referencing the calculate stage job name — verified by grep
- ✓ `cptools2 illum validate --stage 1 --output /path` checks `.npy` file count per plate/channel and exits non-zero on failure — verified by test with missing files
- ✓ Stage base class defines `role`, `gpu`, `memory`, `slots`, `partition()`, `generate_commands()`, `generate_script()`, `validate()` interface

## Dependencies

### Must Complete Before This Phase:
- Phase 1 (Container Integration): `generate_scripts.py` and `commands.py` must emit singularity commands; `containers.py` must resolve roles from manifest

### Blocked By:
- Nothing beyond Phase 1

### Optional:
- Phase 0 (Container Deploy): Having `.sif` on Eddie enables end-to-end testing but not required for code development

## Skills Required (Broad Categories)

- `python-testing`: Unit tests for new partitioning and LoadData extension
- `eddie-resources`: SGE memory sizing (64GB for Stage 1 calculate, 4GB for Stage 2 apply)
- `eddie-job-chaining`: hold_jid dependency between Stage 1 and Stage 2

## Risk Assessment

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| CellProfiler "All" mode memory exceeds 64GB for large plates | Medium | High | Check with `qacct -j` maxrss after first run; increase to `-pe sharedmem 8 -l h_rss=16G` (128GB) if needed |
| `.cppipe` template channel names don't match actual image naming | Medium | Medium | Templates use configurable channel names from YAML; validate with CellProfiler GUI before batch run |
| `parserix` metadata extraction differs between illum and existing workflow | Low | Medium | Reuse existing `loaddata.create_long_loaddata()` for metadata; illum columns are additive only |
| Stage base class over-engineered for current needs | Low | Low | Keep base class minimal — only shared logic (role lookup, script header generation); don't abstract prematurely |

## Assumptions

- `CellProfiler 4.2.8 LoadData format stable`: The LoadData CSV format (FileName_W*, PathName_W*, FileName_Illum_W*, PathName_Illum_W*) is compatible with CellProfiler 4.2.8's CorrectIlluminationApply module — validate by loading a generated CSV in CellProfiler GUI
- `5-channel Cell Painting standard`: DNA, RNA, ER, AGP, Mito channel order is standard for the lab's experiments — but templates should be configurable for different channel sets
- `Per-plate partitioning sufficient`: One CellProfiler job per plate for illumination calculation is correct — CellProfiler's "All" mode needs every image in the plate to compute the correction function

## Notes / Design Decisions

- **Stage base class is lightweight**: It defines the interface but doesn't enforce complex inheritance. Each stage module implements the methods directly. If stages diverge significantly, the base class can be trimmed.
- **Templates are static `.cppipe` files**: Not dynamically generated from Python. Channel names and paths are substituted at generation time, but the CellProfiler module structure is fixed. This keeps templates inspectable in CellProfiler GUI.
- **Illum columns follow CellProfiler convention**: `FileName_Illum_W1`, `PathName_Illum_W1` etc. — this is the naming pattern CellProfiler's CorrectIlluminationApply expects in LoadData CSVs.
- **Validation is a separate subcommand**: `cptools2 illum validate` can be run independently post-job, not just as part of the pipeline. Returns non-zero exit code for scripted checks.

## Source File Change Summary

### New files
- `cptools2/stages/__init__.py` — package init
- `cptools2/stages/base.py` — stage interface: `role`, `gpu`, `memory`, `slots`, `partition()`, `generate_commands()`, `generate_script()`, `validate()`
- `cptools2/stages/illum_calculate.py` — Stage 1: per-plate partitioning, high-memory SGE, `.npy` output
- `cptools2/stages/illum_apply.py` — Stage 2: chunked partitioning with illum columns, hold_jid dependency, PNG output
- `cptools2/stages/validate.py` — post-stage file count validation
- `cptools2/templates/illum_calculate.cppipe` — CellProfiler pipeline for illumination calculation
- `cptools2/templates/illum_apply.cppipe` — CellProfiler pipeline for illumination application

### Modified files
- `cptools2/splitter.py` — add `split_by_plate(plate_store)` function
- `cptools2/loaddata.py` — add `illum_dir` parameter to `create_loaddata()` and `cast_dataframe()`
- `cptools2/__main__.py` — register `illum` subcommand with `calculate`, `apply`, `validate` sub-subcommands
- `cptools2/__init__.py` — expose `stages` package

## Ralph Loops (5)

| Loop | Name | Type | Key Outputs |
|---|---|---|---|
| 210 | Stage base class and package structure | Design/Implementation | `stages/base.py`, `stages/__init__.py`, interface definition |
| 220 | Splitter extension (split_by_plate) | Implementation | Updated `splitter.py`, unit tests for per-plate partitioning |
| 230 | LoadData extension (illum columns) | Implementation | Updated `loaddata.py`, unit tests for `.npy` column injection |
| 240 | CellProfiler pipeline templates and stage modules | Implementation | `.cppipe` templates, `illum_calculate.py`, `illum_apply.py` |
| 250 | CLI wiring, validation, and integration tests | Implementation | Updated `__main__.py`, `validate.py`, end-to-end dry-run test |
