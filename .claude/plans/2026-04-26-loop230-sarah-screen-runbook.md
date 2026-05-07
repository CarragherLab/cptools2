# Loop 230 example-screen Runbook

## Status

`in_progress`

Loop 230 has moved from a small smoke test to a representative staged Eddie
validation using example-screen Cell Painting data.

## Representative Plate

```text
/exports/<college>/datastore/<project>/imagexpress/<screen>/example-plate-001
```

Staging-node check on 2026-04-26:

```text
size: 103G
visible from: -q staging node
not reliably visible from: login / normal compute nodes
```

## Scratch Project

```text
/exports/eddie/scratch/${USER}/cptools2-loop230
```

## Eddie Project and Container Root

Use the existing <group> group project folder as the shared Eddie install
and container location:

```text
${CPTOOLS2_PROJECT_ROOT}
```

Container archives and converted `.sif` files should live under:

```text
${CPTOOLS2_PROJECT_ROOT}/containers
```

Verified on 2026-04-27:

```text
project folder exists
current project folder size: 20M
group mount free space: ~203G
```

Container build and validation on 2026-04-27:

```text
build job: 55197838
build host: node2a05.ecdf.ed.ac.uk
cellprofiler_4.2.8.sif: 1.6G
cellpose_sam_1.0.sif: 7.3G
deepprofiler_1.0.sif: 3.2G
validation job: 55198117
validation host: node1r03.ecdf.ed.ac.uk
validation status: exit_status 0
validated GPU: NVIDIA L40S
```

Note: build job `55197838` created all three `.sif` files but exited `1` during
post-build cleanup because Singularity/SGE exposed a `/local/...` temp path that
the job could not remove. The build script now only removes scratch paths under
`/exports/eddie/scratch/$USER`.

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
input_dir: /exports/<college>/datastore/<project>/imagexpress/<screen>
output_dir: /exports/eddie/scratch/${USER}/cptools2-loop230
plates:
  - example-plate-001
stage_data: true
```

Runtime assets must be scratch-local:

```yaml
illum_pipeline_calculate: /exports/eddie/scratch/${USER}/cptools2-loop230/pipelines/illum_calculate.cppipe
illum_pipeline_apply: /exports/eddie/scratch/${USER}/cptools2-loop230/pipelines/illum_apply.cppipe
seg_pipeline: /exports/eddie/scratch/${USER}/cptools2-loop230/pipelines/nuclear_segmentation.cppipe
feature_extraction:
  tool: deepprofiler
  config: /exports/eddie/scratch/${USER}/cptools2-loop230/config/deepprofiler_config.json
```

## Ralph Loop Breakdown

### Loop 230A: Scratch-contained config

Status: `completed`

Done:

- Added `config/loop230-sarah-screen.yaml`.
- Added `scripts/prepare_loop230_scratch.sh`.
- Generated `/exports/eddie/scratch/${USER}/cptools2-loop230/params.json`.
- Verified params contain explicit `plates`, expanded stages, `stage_data: true`,
  scratch-local pipelines, and resolved `.sif` containers.

### Loop 230B: Staging contract

Status: `completed`

Done:

- `stage_in.nf` keeps DataStore paths as `val`, not Nextflow-staged `path`.
- `stage_in.nf` and `stage_out.nf` use `rsync -rtl`, not `rsync -a`.
- `parse_yaml.py` rejects `/datastore/` inputs with `stage_data: false`.
- example-screen path verified from `-q staging`.

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
   source ${CPTOOLS2_PROJECT_ROOT}/activate.sh
   export NXF_OPTS='-Xms128m -Xmx512m -XX:+UseSerialGC -XX:ActiveProcessorCount=1'
   cd ~/cptools2-loop230
   nextflow run nextflow/main.nf \
     -profile eddie \
     -params-file /exports/eddie/scratch/${USER}/cptools2-loop230/params.json \
     --stages illum \
     -ansi-log false
   ```

3. Verify staged and illumination outputs in scratch before running segmentation
   and feature extraction.

### Loop 230D: Local container and architecture smoke

Status: `completed`

Done:

- Built local Docker images:
  - `cellprofiler/cellprofiler:4.2.8`
  - `cptools2/deepprofiler:1.0`
  - `cptools2/cellpose_sam:1.0`
- Fixed the DeepProfiler container so the upstream source tree is available on
  `PYTHONPATH`; the packaged wheel omits nested modules required by
  `python -m deepprofiler`.
- Aligned the Cellpose image tag with Nextflow config: `cellpose_sam`, not
  `cellpose-sam`.
- Removed the unused `cellpose[gpu]` pip extra from the Cellpose Dockerfile.
- Added smoke tests for the Nextflow/container contract and Loop 230 scratch
  staging assumptions.
- Verified Nextflow `test` and `eddie` profiles with Dockerized Nextflow
  config parsing.

## Known Operational Constraints

- Staging queue is single-slot copy/list only. Do not request `sharedmem` or GPUs
  there.
- Compute and GPU processes must only touch Eddie filesystem data after staging.
- Eddie GPU request is `-q gpu -l gpu=1`; `gpus` is non-requestable. If a GPU
  task needs multiple CPU slots, use the standard sharedmem PE for those slots,
  not `gpu-a100` on the `gpu` queue.
- The representative plate is 103G before derived outputs, so scratch usage must
  be checked before full compute.

## Verification So Far

```text
Docker Desktop reachable locally: 29.3.1
CellProfiler container: 4.2.8
DeepProfiler container: CLI starts; TensorFlow 2.5.3 sees local RTX 4500 via Docker GPU
Cellpose-SAM container: Cellpose 4.1.1; Torch 2.7.1+cu118 sees local RTX 4500
Eddie containers: built in ${CPTOOLS2_PROJECT_ROOT}/containers
Eddie container validation: job 55198117 passed; Cellpose and DeepProfiler saw NVIDIA L40S
Nextflow config smoke: test and eddie profiles parse with nextflow/nextflow:24.10.4
Architecture smoke tests: tests/test_nextflow_architecture_smoke.py passed
Eddie install: completed
Scratch prep: completed
Focused parser tests: passed locally where no tmp_path fixture needed
Manual parser contract checks: passed
Eddie pytest: blocked because runtime env lacks pytest
Local full pytest: blocked by Windows temp/cache permission errors
Nextflow config/run on Eddie: blocked by login-node thread pressure
```
