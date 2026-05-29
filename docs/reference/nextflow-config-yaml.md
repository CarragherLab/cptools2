# Nextflow Config YAML

This file explains the YAML keys used by `cptools2 pipeline` for the
Nextflow-first Eddie workflow.

## Example

See `config/loop230-sarah-screen.yaml` for the representative Loop 230 Eddie
test config.

## Parameters

### `input_dir`

Root directory containing plate folders.

For example-screen this is the DataStore root:

```yaml
input_dir: /exports/<college>/datastore/<project>/imagexpress/<screen>
```

DataStore paths are visible from Eddie staging nodes, not from ordinary compute
jobs. Do not point compute stages at this path directly.

Legacy alias: `experiment`.

### `output_dir`

Scratch project directory for this run.

Everything generated for the run should live under this directory, including
`params.json`, copied pipeline files, copied config files, command files,
staged images, work outputs, and published results.

```yaml
output_dir: /exports/eddie/scratch/${USER}/cptools2-loop230
```

Legacy alias: `location`.

### `plates`

Explicit plate IDs to process.

Use this for DataStore runs because login nodes cannot reliably glob DataStore
paths. `plates` may be a YAML list or a comma-separated string.

```yaml
plates:
  - example-plate-001
```

Legacy alias: `plate_list`.

### `stage_data`

Whether to stage images from DataStore to Eddie storage before compute.

For Eddie DataStore runs this must be `true`. Staging jobs run on `-q staging`
and only copy/list data. They must not request multiple cores or perform image
analysis.

```yaml
stage_data: true
```

### `illum_pipeline_calculate`

Full path to the CellProfiler illumination-calculate pipeline used by the
Nextflow `ILLUM_CALCULATE` process.

For self-contained runs, copy this file into the scratch project:

```yaml
illum_pipeline_calculate: /exports/eddie/scratch/${USER}/cptools2-loop230/pipelines/illum_calculate.cppipe
```

### `illum_pipeline_apply`

Full path to the CellProfiler illumination-apply pipeline used by the Nextflow
`ILLUM_APPLY` process.

```yaml
illum_pipeline_apply: /exports/eddie/scratch/${USER}/cptools2-loop230/pipelines/illum_apply.cppipe
```

### `seg_pipeline`

Full path to the CellProfiler segmentation pipeline used by the Nextflow
`SEGMENTATION` process.

```yaml
seg_pipeline: /exports/eddie/scratch/${USER}/cptools2-loop230/pipelines/nuclear_segmentation.cppipe
```

### `pipeline`

Legacy single-pipeline path retained for compatibility with older cptools2
configuration parsing.

The current Nextflow workflow uses the more specific `illum_pipeline_calculate`,
`illum_pipeline_apply`, and `seg_pipeline` keys. Keep `pipeline` as a full
scratch-local path until the legacy parser is removed.

### `commands location`

Directory for generated command artifacts.

For the Nextflow workflow this should be inside `output_dir`, usually:

```yaml
commands location: /exports/eddie/scratch/${USER}/cptools2-loop230/commands
```

If omitted, cptools2 defaults it to `<output_dir>/commands`.

### Batch cleanup policy

`cptools2 pipeline` cleans per-batch Nextflow work directories with a cleanup
policy. The default is `verified`:

```bash
cptools2 pipeline config.yaml --cleanup-policy verified
```

Policies:

- `verified`: clean `work/batch_###` only after all stage-out verification
  artifacts in that batch report `verification_status: verified`.
- `success`: clean after a successful Nextflow batch unless stage-out evidence
  explicitly reports `failed` or `unverified`.
- `keep`: preserve batch work for diagnostics.

The legacy `--clean-work` flag is retained as a compatibility alias for
`--cleanup-policy success`, preserving the old behavior where a successful
batch can be cleaned even if no verification artifact is present.

Cleanup is restricted to exact batch work directories under
`<output_dir>/work/batch_###`. Failed or unverified batches are preserved.

### Stage-out evidence

`STAGE_OUT` uses `rsync` as the transfer authority and writes a small evidence
bundle for each stage-out task:

```text
<output_dir>/stage_out_evidence/<batch_name>/<plate_or_artifact_id>/
  rsync.log
  verification.json
```

`verification.json` records the plate or artifact id, destination, rsync exit
code, rsync log path, required artifacts, required artifact presence, and
`verification_status`. The same evidence bundle is also emitted work-locally by
the Nextflow process, but the durable copy under `<output_dir>` is the audit
trail that survives verified cleanup of `work/batch_###`.

For DeepProfiler feature exports, required artifacts include the exported CSV
tables and `feature_export_status.csv`. Full source/destination manifest
comparison is intentionally not part of the default verification path; rsync
exit status plus required destination artifacts is the cleanup gate.

Each pipeline run also writes `<output_dir>/batch_status.csv`. This ledger
records batch id, plates, Nextflow status, stage-out status, cleanup policy,
cleanup status, work directory, scratch availability before and after the
batch, params path, trace path, attempt, timestamps, and a message. Use this
file first when deciding whether scratch work was cleaned, preserved for
diagnostics, or blocked by verification.

Each non-dry-run pipeline execution also writes `<output_dir>/run_report.md`.
The report summarises batch status counts, stage-out evidence location,
cleanup status, and any problem batch records. It is written before final
failure exits as well as after normal completion, so users do not need to infer
state from an empty output directory.

By default, a failed batch stops the driver before the next batch. For
multi-plate production runs where independent later batches should still be
allowed to finish, use:

```bash
cptools2 pipeline config.yaml --continue-on-batch-failure
```

With this flag, Nextflow failures, unverified stage-out, and cleanup failures
are recorded in `batch_status.csv` and `run_report.md`; the failed batch work
directory is preserved, and the driver proceeds to later independent batches.
The final process exit code remains non-zero if any batch failed or could not
be verified.

### Eddie production launcher

On Eddie, launch the Nextflow driver from a login `tmux` session, not as a qsub
driver job. Nextflow remains responsible for submitting the actual SGE process
jobs. Running the driver inside another scheduler job makes the driver harder
to inspect and can hide the long-lived orchestration state we need for
multi-batch recovery.

Use the Phase 3.1 launcher:

```bash
scripts/eddie_phase31_launch.sh \
  --config /path/to/config.yaml \
  --run-root /exports/eddie/scratch/$USER/cptools2-ai-update/diagnostics/phase-3.1-three-plate \
  --diagnostics minimal \
  --cleanup-policy verified \
  --continue-on-batch-failure
```

The launcher:

- refuses to reuse an existing tmux session name
- sources `config/eddie_env.sh`
- sets run-local `CPTOOLS2_SCRATCH_ROOT`, `NXF_HOME`, container caches, and temp
  directories under the run root
- writes `driver_env/env.before.txt`, module, `qstat`, and process snapshots
- writes `status.txt`, `driver_status.json`, and `launcher.log`
- starts `cptools2 pipeline` in tmux with the requested diagnostics and cleanup
  policy

Start the first three-plate production acceptance with `--diagnostics minimal`.
If that passes, retest `full` diagnostics separately before restoring full
observer output as the default recommendation for Eddie scale-up.

### Phase 3.1 pre-acceptance: three-plate forced batching

Dry-run batching has passed for the three real plates `3723-D-30`,
`3723-D-300`, and `3723-D-100`. Each measured about `100.521 GB`, and a very
small `scratch_utilisation_fraction` was used to force three separate batches
so operator evidence could be checked batch by batch.

Expected split:

- `batch_001`: `3723-D-30`
- `batch_002`: `3723-D-300`
- `batch_003`: `3723-D-100`

Treat this as pre-acceptance evidence, not a completed production run. The dry
run used the Nextflow tmux driver, not `qsub`, and the first check sequence was
minimal by design:

- fresh scratch run root
- tmux session from the production launcher
- minimal Nextflow diagnostics first
- `qstat` snapshot
- launcher log and process snapshot
- `batch_status.csv`
- `stage_out_evidence`

The forced-batching recipe exists to prove the batch ledger, cleanup gate, and
durable stage-out path before the real acceptance run. Real execution must wait
until the diagnostics modes and launcher hardening from Loops 580 and 590 are
implemented.

Concrete acceptance recipe for the three-plate forced-batching pre-acceptance
run:

```yaml
plates:
  - 3723-D-100
  - 3723-D-30
  - 3723-D-300
plate_sizes_gb:
  3723-D-100: 100.521192
  3723-D-30: 100.521200
  3723-D-300: 100.521198
scratch_utilisation_fraction: 0.0001
scratch_work_factor: 1.3
```

This `0.0001` value is a forced-batching acceptance setting, not a production
default. It exists only to make the three real plates split into the expected
acceptance batches:

- `batch_001`: `3723-D-30`
- `batch_002`: `3723-D-300`
- `batch_003`: `3723-D-100`

### `channels`

Logical channel names for the assay.

The defaults below match the current Cell Painting assumptions used by the
pipeline templates and downstream feature-extraction expectations.

```yaml
channels:
  - DNA
  - RNA
  - ER
  - AGP
  - Mito
```

### `stages`

Which workflow stages to run.

Aliases are expanded by cptools2:

```yaml
stages:
  - illum
  - segment
  - extract
```

`illum` expands to `illum_calculate` and `illum_apply`.
`segment` expands to `segmentation`.
`extract` expands to `feature_extract`.

### `segmentation`

Segmentation settings.

Current Loop 230 execution still uses the CellProfiler segmentation module.
Cellpose is a core target for the revamp, but not yet the active Nextflow
segmentation engine.

```yaml
segmentation:
  model: cellprofiler
```

If `diameter` is provided, cptools2 maps it to the Nextflow segmentation
diameter parameters.

### `feature_extraction`

Feature-extraction engine settings.

```yaml
feature_extraction:
  tool: deepprofiler
  config: ${CPTOOLS2_PROJECT_ROOT}/cptools2/templates/deepprofiler_config.json
  weights: ${CPTOOLS2_MODEL_DIR}/deepprofiler/Cell_Painting_CNN_v1.hdf5
  batch_size: 128
```

`tool` maps to `params.feature_extraction_tool`.
`config` maps to `params.feature_extraction_config`.
`weights` maps to `params.feature_extraction_weights`.
`batch_size` maps to `params.feature_extraction_batch_size`.

For Eddie, model weights are a local site asset, not a Git-tracked artifact.
`config/eddie_env.sh` exports `CPTOOLS2_MODEL_DIR`; `nextflow/conf/eddie.config`
passes that directory into the Singularity runtime. The default Cell Painting
checkpoint contract is `deepprofiler/Cell_Painting_CNN_v1.hdf5` under that model
root.

### `feature_export`

Feature export converts native extractor outputs into measurement tables.

```yaml
feature_export:
  enabled: true
  formats:
    - csv
```

CSV is required for the feature-export gate. Parquet may be added as an extra
format:

```yaml
feature_export:
  enabled: true
  formats:
    - csv
    - parquet
```

Exported metadata columns use `Metadata_*` names.

On Eddie, the CPU export process uses the configured project virtualenv Python
by default. That environment must include `numpy`, `polars`, and the cptools2
runtime dependencies.

### `container_path` and `containers`

Container resolution inputs.

For Eddie, the installed activation script sets `CPTOOLS2_CONTAINER_DIR`.
cptools2 then resolves expected `.sif` files from that directory. Explicit
container paths can still be supplied when needed, but Loop 230 should use the
shared Chandran container directory through the activation script.

## Data Flow

```text
DataStore plate path
  -> STAGE_IN on -q staging, single slot, rsync -rtl
  -> staged images in Eddie work directory
  -> compute/GPU stages on Eddie filesystem
  -> STAGE_OUT on -q staging, single slot, rsync -rtl
  -> output_dir plate results
```

The key rule: staging nodes move data, compute nodes process staged data.
