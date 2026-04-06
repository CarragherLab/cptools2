# Phase 4: Feature Extraction Stage

## Objective

Add a role-agnostic feature extraction stage to cptools2 that generates GPU-enabled SGE scripts for DeepProfiler (and future tools like DINOv2), producing single-cell morphological feature embeddings from illumination-corrected images and nucleus centroid locations.

## Scope

### Included:
- Stage module: `cptools2/stages/feature_extract.py` — role-agnostic, reads `tool` from YAML config to determine which container role to use
- DeepProfiler config template: `cptools2/templates/deepprofiler_config.json`
- DeepProfiler project directory scaffolding (index.csv, location file renaming, symlink creation)
- CLI: `cptools2 extract config.yml`
- GPU-specific SGE script generation (`-pe gpu-a100 1 -l gpus=1`, `--nv` flag)
- `commands.py`: Use `container_command()` (from Phase 1) for DeepProfiler invocations
- Post-stage validation (`.npz` file count, feature dimensionality check, NaN/Inf detection)
- YAML config section: `feature_extraction.tool` to select container role

### Explicitly NOT included:
- Pipeline orchestrator (Phase 5)
- DINOv2 implementation (future — only the extension point is established here)
- Downstream normalisation/aggregation (pycytominer — separate tool)
- Model training (this phase is inference-only using pre-trained weights)

## Key Deliverables

| Deliverable | Format | Location |
|---|---|---|
| Feature extraction stage module | Python | `cptools2/stages/feature_extract.py` |
| DeepProfiler config template | JSON | `cptools2/templates/deepprofiler_config.json` |
| Updated __main__.py | Python | `cptools2/__main__.py` |
| Updated validate.py | Python | `cptools2/stages/validate.py` |
| Updated parse_yaml.py | Python | `cptools2/parse_yaml.py` |
| Feature extraction tests | Python | `tests/test_feature_extract.py` |

## Success Criteria

- ✓ `cptools2 extract config.yml` generates SGE scripts with `-pe gpu-a100 1 -l gpus=1 -l h_rss=16G` — verified by grep on generated .sh
- ✓ Generated scripts include `singularity exec --nv` (GPU passthrough) — verified by grep
- ✓ Generated scripts include `-hold_jid` referencing the segmentation stage job name — verified by grep
- ✓ The container role is determined by `config.feature_extraction.tool` (e.g., `deepprofiler`) and resolved via `containers.resolve_container_path()` — verified by test with different role names
- ✓ `deepprofiler_config.json` template contains correct channel order (DNA, RNA, ER, AGP, Mito), box_size=128, feature_layer=`block6a_activation` — verified by inspection
- ✓ Changing `feature_extraction.tool` from `deepprofiler` to another role (e.g., `dinov2`) in the YAML config changes the container used without any Python code changes — verified by test
- ✓ Validation checks `.npz` file count matches expected sites, feature dimensionality is 672 (for DeepProfiler), no NaN/Inf values — verified by test with fixture data
- ✓ DeepProfiler project directory structure (inputs/images, inputs/metadata, outputs/checkpoint) is scaffolded correctly — verified by test

## Dependencies

### Must Complete Before This Phase:
- Phase 1 (Container Integration): `container_command()` and GPU flag logic in `containers.py`
- Phase 2 (Illumination Correction): Corrected images exist as input
- Phase 3 (Segmentation): Centroid CSV files exist as input

### Blocked By:
- Phase 0 (Container Deploy): GPU container (deepprofiler_1.0.sif) must exist on Eddie for end-to-end testing
- Pre-trained model weights: `combinedset_cellsout_e30.hdf5` must be downloaded from Zenodo and placed in the project directory

### Optional:
- Nothing

## Skills Required (Broad Categories)

- `python-testing`: Unit tests for stage module and validation
- `eddie-resources`: GPU job sizing (`-pe gpu-a100`, `-l gpus`, `--nv`, h_rss for GPU nodes)
- `eddie-job-chaining`: hold_jid from feature extraction to segmentation

## Risk Assessment

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| GPU queue time on Eddie delays validation | High | Low | GPU validation can be deferred; code is testable locally without GPU. Process multiple plates per GPU job to reduce scheduling overhead. |
| DeepProfiler project structure requirements change between versions | Low | Medium | Pin DeepProfiler version in container; template structure matches current handbook |
| Feature dimensionality assumption (672) is wrong for different models | Low | Low | Validation accepts expected_dims parameter; 672 is only the default for the Cell Painting CNN |
| `--nv` flag silent fallback to CPU | Medium | High | Validation should check that GPU was actually used (TensorFlow logs or runtime duration check). Add a GPU detection assertion at start of DeepProfiler job. |
| Pre-trained weights not accessible from compute nodes | Low | Medium | Weights stored in group space alongside containers; bind-mounted into container |

## Assumptions

- `DeepProfiler CLI interface stable`: `python deepprofiler --root ... --config ... --exp ... profile` is the correct invocation — validate against current DeepProfiler GitHub
- `Cell Painting CNN produces 672-dim embeddings`: Extraction from `block6a_activation` layer of EfficientNet-B0 produces 672 features — documented in Moshkov et al. 2024
- `A100 80GB VRAM sufficient`: Batch size 128 with 5-channel 128x128 crops fits comfortably in 80GB VRAM — validate during first run
- `Location CSV renaming convention`: DeepProfiler expects location files named `{Plate}-{Well}-{Site}.csv` — must be validated against current handbook

## Notes / Design Decisions

- **Role-agnostic design**: The stage module reads `feature_extraction.tool` from config and resolves the container via `containers.resolve_container_path(yaml_dict, role=tool_name)`. No DeepProfiler-specific code in the container resolution path. Adding DINOv2 later means: (1) build and deploy a DINOv2 container, (2) add manifest entry, (3) set `tool: dinov2` in config, (4) write a DINOv2-specific command generator in `feature_extract.py` (the only code change).
- **DeepProfiler project scaffolding**: DeepProfiler has strict directory layout requirements (inputs/images/, inputs/metadata/locations/, etc.). The stage module scaffolds this structure and creates symlinks to the corrected images and renamed centroid files. This avoids copying large image data.
- **GPU job sizing**: One GPU per plate is sufficient for inference. Processing multiple plates per job reduces GPU queue overhead. The stage supports both one-plate-per-job and multi-plate-per-job modes.
- **Pre-trained weights management**: The Cell Painting CNN weights (20MB) are stored in Eddie group space alongside the containers. They are bind-mounted into the container at runtime. The weights path is configurable in the DeepProfiler config template.

## Source File Change Summary

### New files
- `cptools2/stages/feature_extract.py` — Feature extraction stage: role from config, gpu=True, memory=16G, gpu-a100 PE, per-plate partitioning, hold_jid on segmentation, DeepProfiler project scaffolding
- `cptools2/templates/deepprofiler_config.json` — DeepProfiler config with channel order, box_size, feature_layer, checkpoint path

### Modified files
- `cptools2/__main__.py` — register `extract` subcommand
- `cptools2/stages/validate.py` — add `validate_features()`: check .npz count, feature dims, NaN/Inf
- `cptools2/stages/__init__.py` — expose feature_extract stage
- `cptools2/parse_yaml.py` — parse `feature_extraction` config section (tool name, parameters)

## Ralph Loops (4)

| Loop | Name | Type | Key Outputs |
|---|---|---|---|
| 410 | Feature extraction stage module | Implementation | `stages/feature_extract.py` with role-agnostic container lookup, GPU SGE script generation |
| 420 | DeepProfiler config template and project scaffolding | Implementation | `templates/deepprofiler_config.json`, directory scaffolding, symlink creation, location file renaming |
| 430 | Config parsing and CLI wiring | Implementation | Updated `parse_yaml.py` (feature_extraction section), updated `__main__.py` (extract subcommand) |
| 440 | Validation and tests | Implementation | Updated `validate.py` (.npz checks), unit tests, integration test with fixture data |
