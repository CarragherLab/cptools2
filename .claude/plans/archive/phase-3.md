# Phase 3: Segmentation Stage

## Objective

Add nuclear segmentation as a pipeline stage in cptools2, generating SGE scripts that run CellProfiler's IdentifyPrimaryObjects on illumination-corrected images to produce per-site centroid CSV files suitable for DeepProfiler input.

## Scope

### Included:
- Stage module: `cptools2/stages/segmentation.py`
- CellProfiler pipeline template: `cptools2/templates/nuclear_segmentation.cppipe`
- CLI: `cptools2 segment config.yml`
- SGE script generation with `-hold_jid` referencing the illum_apply stage
- Post-stage validation (centroid CSV existence, column format check)
- Optional: post-processing minimum nucleus distance filter

### Explicitly NOT included:
- Cellpose-SAM segmentation (future stage — would use the `cellpose` container role)
- Feature extraction (Phase 4)
- Pipeline orchestrator (Phase 5)
- Cell body segmentation (design decision: nucleus-only for DeepProfiler workflow)

## Key Deliverables

| Deliverable | Format | Location |
|---|---|---|
| Segmentation stage module | Python | `cptools2/stages/segmentation.py` |
| Nuclear segmentation pipeline | CellProfiler .cppipe | `cptools2/templates/nuclear_segmentation.cppipe` |
| Updated __main__.py | Python | `cptools2/__main__.py` |
| Updated validate.py | Python | `cptools2/stages/validate.py` |
| Segmentation tests | Python | `tests/test_segmentation.py` |

## Success Criteria

- ✓ `cptools2 segment config.yml` generates SGE scripts with `-l h_rss=4G` (single slot) and `singularity exec` — verified by grep on generated .sh
- ✓ Generated script includes `-hold_jid` referencing the illum_apply stage job name — verified by grep
- ✓ `nuclear_segmentation.cppipe` contains IdentifyPrimaryObjects on CorrDNA channel with Otsu thresholding — verified by inspection
- ✓ `nuclear_segmentation.cppipe` ExportToSpreadsheet outputs `Nuclei_Location_Center_X` and `Nuclei_Location_Center_Y` columns — verified by inspection (these exact column names are required by DeepProfiler)
- ✓ Segmentation stage uses `role = "cellprofiler"` and `gpu = False` — verified by code inspection
- ✓ Validation checks that centroid CSV files exist for every site and contain the required columns — verified by test with fixture data
- ✓ Segmentation stage reuses existing chunked partitioning (not per-plate) — verified by code inspection

## Dependencies

### Must Complete Before This Phase:
- Phase 1 (Container Integration): Singularity command generation working
- Phase 2 (Illumination Correction): Stage base class exists; illum_apply produces corrected PNG images that segmentation consumes

### Blocked By:
- Nothing beyond Phase 2

### Optional:
- Phase 0 containers on Eddie: enables end-to-end testing

## Skills Required (Broad Categories)

- `python-testing`: Unit tests for segmentation stage and validation
- `eddie-job-chaining`: hold_jid from segmentation to illum_apply

## Risk Assessment

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| IdentifyPrimaryObjects parameters need tuning per cell line | High | Medium | Template uses reasonable defaults (20-80px diameter, Otsu); document that parameters should be validated in CellProfiler GUI per cell line before batch run |
| Corrected image naming doesn't match template regex | Medium | Medium | Metadata extraction regex in `.cppipe` must match illum_apply output naming; validate by running Stage 2 → Stage 3 on one plate |
| Centroid CSV column names don't match DeepProfiler expectations | Low | High | Explicitly configure ExportToSpreadsheet to output exact column names; validate against DeepProfiler handbook format spec |

## Assumptions

- `CorrDNA channel sufficient for segmentation`: Nuclear segmentation on the DNA/DAPI channel is robust enough across cell lines — validated by the design decision documented in Guide 02 (nucleus-only approach)
- `Single-slot 4GB sufficient`: Per-image segmentation is not memory-intensive; 4GB RSS per job is adequate — validate with `qacct -j` maxrss after first run
- `Chunked partitioning appropriate`: Unlike illumination correction (which needs all images per plate), segmentation is embarrassingly parallel per-image — standard chunk-based splitting is correct

## Notes / Design Decisions

- **Nucleus-only segmentation**: Deliberate choice documented in `dev_docs/new_pipeline_focus/guide_02_cellprofiler_segmentation.md`. DeepProfiler needs only centroid locations, not cell body masks. Full cell body segmentation adds parameter tuning complexity across morphologically diverse cell lines.
- **Minimum distance filter is optional post-processing**: The `filter_by_min_distance()` function (documented in Guide 02) removes closely-spaced nuclei that would produce overlapping 128x128 crops in DeepProfiler. This is a Python post-processing step, not part of the CellProfiler pipeline, and is configurable via the YAML config.
- **Same container as illumination correction**: Both stages use `cellprofiler_4.2.8.sif`. No additional container needed.
- **Lightweight stage module**: Segmentation stage follows the same pattern as illum stages but with simpler resource requirements (single slot, low memory, no sharedmem PE).

## Source File Change Summary

### New files
- `cptools2/stages/segmentation.py` — Segmentation stage: role=cellprofiler, gpu=False, memory=4G, single slot, chunked partitioning, hold_jid on illum_apply
- `cptools2/templates/nuclear_segmentation.cppipe` — IdentifyPrimaryObjects on CorrDNA, ExportToSpreadsheet with centroid columns

### Modified files
- `cptools2/__main__.py` — register `segment` subcommand
- `cptools2/stages/validate.py` — add `validate_segmentation()`: check centroid CSV existence, column names, row counts
- `cptools2/stages/__init__.py` — expose segmentation stage

## Ralph Loops (3)

| Loop | Name | Type | Key Outputs |
|---|---|---|---|
| 310 | Segmentation stage module | Implementation | `stages/segmentation.py` with partitioning, command generation, script generation |
| 320 | CellProfiler segmentation template | Implementation | `nuclear_segmentation.cppipe` with IdentifyPrimaryObjects and ExportToSpreadsheet |
| 330 | CLI wiring, validation, and tests | Implementation | Updated `__main__.py`, validation checks, unit tests |
