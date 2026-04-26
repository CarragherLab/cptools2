# Loop 230 Sarah-screen Runbook

## Status

`in_progress`

Loop 230 has moved from a small smoke test to a representative staged Eddie
validation using Sarah-screen Cell Painting data.

## Representative Plate

```text
/exports/igmm/datastore/ImageXpress2020/imagexpress/Sarah-screen/3723-D-100
```

Staging-node check on 2026-04-26:

```text
size: 103G
visible from: -q staging node
not reliably visible from: login / normal compute nodes
```

## Scratch Project

```text
/exports/eddie/scratch/mharvey2/cptools2-loop230
```

Prepared files:

```text
config/loop230-sarah-screen.yaml
config/deepprofiler_config.json
params.json
pipelines/illum_apply.cppipe
pipelines/illum_calculate.cppipe
pipelines/nuclear_segmentation.cppipe
```

## Config Contract

DataStore-backed YAML configs must stage data:

```yaml
input_dir: /exports/igmm/datastore/ImageXpress2020/imagexpress/Sarah-screen
output_dir: /exports/eddie/scratch/mharvey2/cptools2-loop230
plates:
  - 3723-D-100
stage_data: true
```

Runtime assets must be scratch-local:

```yaml
illum_pipeline_calculate: /exports/eddie/scratch/mharvey2/cptools2-loop230/pipelines/illum_calculate.cppipe
illum_pipeline_apply: /exports/eddie/scratch/mharvey2/cptools2-loop230/pipelines/illum_apply.cppipe
seg_pipeline: /exports/eddie/scratch/mharvey2/cptools2-loop230/pipelines/nuclear_segmentation.cppipe
feature_extraction:
  tool: deepprofiler
  config: /exports/eddie/scratch/mharvey2/cptools2-loop230/config/deepprofiler_config.json
```

## Ralph Loop Breakdown

### Loop 230A: Scratch-contained config

Status: `completed`

Done:

- Added `config/loop230-sarah-screen.yaml`.
- Added `scripts/prepare_loop230_scratch.sh`.
- Generated `/exports/eddie/scratch/mharvey2/cptools2-loop230/params.json`.
- Verified params contain explicit `plates`, expanded stages, `stage_data: true`,
  scratch-local pipelines, and resolved `.sif` containers.

### Loop 230B: Staging contract

Status: `completed`

Done:

- `stage_in.nf` keeps DataStore paths as `val`, not Nextflow-staged `path`.
- `stage_in.nf` and `stage_out.nf` use `rsync -rtl`, not `rsync -a`.
- `parse_yaml.py` rejects `/datastore/` inputs with `stage_data: false`.
- Sarah-screen path verified from `-q staging`.

### Loop 230C: Representative staged run

Status: `blocked`

Reason:

- Eddie login node `login01` had `ulimit -u=200`.
- Existing Nextflow Java drivers were using about 182 threads.
- New Java/Nextflow startup failed with native-thread errors before config
  validation could run.

Needed:

1. Wait for existing Nextflow drivers to finish, or run the driver from a
   controlled session with enough thread headroom.
2. Start with staged illumination:

   ```bash
   source /exports/cmvm/eddie/scs/groups/chandranlabs/cptools2/activate.sh
   export NXF_OPTS='-Xms128m -Xmx512m -XX:+UseSerialGC -XX:ActiveProcessorCount=1'
   cd ~/cptools2-loop230
   nextflow run nextflow/main.nf \
     -profile eddie \
     -params-file /exports/eddie/scratch/mharvey2/cptools2-loop230/params.json \
     --stages illum \
     -ansi-log false
   ```

3. Verify staged and illumination outputs in scratch before running segmentation
   and feature extraction.

## Known Operational Constraints

- Staging queue is single-slot copy/list only. Do not request `sharedmem` or GPUs
  there.
- Compute and GPU processes must only touch Eddie filesystem data after staging.
- Eddie GPU request is `-pe gpu-a100 1 -l gpu=1`; `gpus` is non-requestable.
- The representative plate is 103G before derived outputs, so scratch usage must
  be checked before full compute.

## Verification So Far

```text
Docker Desktop reachable locally: 29.3.1
Eddie install: completed
Scratch prep: completed
Focused parser tests: passed locally where no tmp_path fixture needed
Manual parser contract checks: passed
Eddie pytest: blocked because runtime env lacks pytest
Local full pytest: blocked by Windows temp/cache permission errors
Nextflow config/run on Eddie: blocked by login-node thread pressure
```
