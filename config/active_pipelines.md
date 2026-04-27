# Active Pipelines

## Pipeline: cptools2 Nextflow Eddie Validation

**Description:** Validate the revamped cptools2 Nextflow pipeline on Eddie using real high-content imaging data.
**Status:** testing
**Last updated:** 2026-04-27

### Current Phase

Phase 2, Loop 230: End-to-End Eddie Test.

### Stages

| # | Component | Purpose | Status |
|---|---|---|---|
| 1 | `scripts/install_eddie.sh` | Install shared cptools2 environment and project structure on Eddie | pending |
| 2 | `cptools2 pipeline <config.yml> --dry-run` | Generate and inspect `params.json` without running Nextflow | pending |
| 3 | `nextflow/modules/stage_in.nf` | Stage plate data from DataStore to Eddie scratch | implemented, needs Eddie validation |
| 4 | `nextflow/modules/illum_calculate.nf` | Calculate illumination correction functions | implemented, needs Eddie validation |
| 5 | `nextflow/modules/illum_apply.nf` | Apply illumination correction | implemented, needs Eddie validation |
| 6 | `nextflow/modules/segmentation.nf` | Run current segmentation stage | implemented, needs Eddie validation |
| 7 | `nextflow/modules/feature_extract.nf` | Run current feature extraction stage | implemented, needs Eddie validation |
| 8 | `nextflow/modules/stage_out.nf` | Stage final outputs back to DataStore | implemented, needs Eddie validation |

### Dependency Graph

```text
install_eddie
  -> dry_run
  -> stage_in
  -> illum_calculate
  -> illum_apply
  -> segmentation
  -> feature_extract
  -> stage_out
```

### Key Paths

```text
CONTAINER_DIR=/exports/cmvm/eddie/smgphs/groups/ChandranLabs/cptools2/containers
PROJECT_ROOT=/exports/cmvm/eddie/smgphs/groups/ChandranLabs/cptools2
DATASTORE_ROOT=/exports/cmvm/datastore/smgphs/groups/ChandranLabs
SCRATCH_PROJECT=/exports/eddie/scratch/$USER/cptools2
```

### Planned AI Engine Expansion

Future phases should add modular Nextflow engines for:

- Cellpose segmentation.
- DeepProfiler learned feature extraction.
- Cell-DINO embeddings for standard 5-channel Cell Painting.
- uniDINO embeddings for variable-channel fluorescence assays.
- DINOv2 baseline embeddings for phase contrast and brightfield.

The immediate validation loop should not absorb the full AI roadmap. It should prove that the Eddie substrate works.
