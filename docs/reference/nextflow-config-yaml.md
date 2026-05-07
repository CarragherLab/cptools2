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
