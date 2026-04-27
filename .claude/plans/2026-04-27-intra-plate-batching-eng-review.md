# Plan Engineering Review: Intra-Plate Batching

## Review Scope

Reviewed:

- `.claude/plans/2026-04-27-intra-plate-batching-design.md`
- `.claude/plans/phase-2.5-intra-plate-batching.md`
- Current implementation points in `cptools2/batch.py`, `cptools2/parse_yaml.py`, `cptools2/filelist.py`, `cptools2/splitter.py`, `cptools2/loaddata.py`, `nextflow/main.nf`, and `nextflow/conf/eddie.config`

## Verdict

The plan is technically sound and ready to become executable Ralph loops, with three required refinements:

1. Make active plate-batch selection a first-class Nextflow param before chunk fan-out work starts.
2. Add explicit subset controls for Eddie smoke tests, for example `max_chunks` or `chunk_limit`.
3. Add hard validation for missing channels and AI segmentation compatibility.

No critical architecture blocker remains.

## What Already Exists

| Existing piece | Reuse decision |
|----------------|----------------|
| `cptools2/batch.py` scratch-aware plate batching | Reuse for outer plate batching. |
| `cptools2/parse_yaml.py` `chunk` parsing | Reuse and extend so `chunk_size` reaches Nextflow cleanly. |
| `cptools2/filelist.py` parserix-backed ImageXpress discovery | Reuse inside helper scripts called by Nextflow. |
| `cptools2/splitter.py` image-set chunking idea | Reuse the behaviour and tests, not the SGE command generation. |
| `cptools2/loaddata.py` LoadData-compatible metadata output | Reuse where CellProfiler needs it. |
| `nextflow/conf/eddie.config` staging/GPU labels | Reuse, add throttling for GPU chunk fan-out. |

## Architecture Review

### Issue 1: Active Plate Batch Must Be Real Execution State

Current `cmd_pipeline` computes batches, but the existing design notes that the active batch is not yet passed into each Nextflow invocation. This must be Loop 260 and should happen before chunk fan-out. Otherwise the pipeline can silently rerun the same plate set for each computed batch.

Decision: make `params.plates` batch-specific for every Nextflow call and assert it in dry-run output.

### Issue 2: Nextflow-Owned Chunking Still Needs Script Boundaries

Pure Groovy filename parsing would be brittle. The updated design correctly keeps Nextflow as the orchestration owner and uses cptools2/parserix helper scripts as process tools. This is the right split.

Decision: add explicit `BUILD_IMAGESET_INDEX` and `CHUNK_IMAGESETS` processes before chunked analysis stages.

### Issue 3: AI Segmentation Contract Must Be Enforced

The user clarified that Cellpose is required for AI pipelines but not pure CellProfiler. The plan now needs validation so DeepProfiler, Cell-DINO, and uniDINO cannot run without Cellpose segmentation outputs.

Decision: config validation and Nextflow branching tests must reject invalid AI feature extraction paths.

## Test Review

Required test coverage before Eddie scaling:

```text
YAML config
  -> active batch plate list
  -> STAGE_IN selected plate
  -> BUILD_IMAGESET_INDEX
  -> CHUNK_IMAGESETS
  -> chunked stages
```

Required tests:

- `97` image sets with chunk size `96` yields two chunks.
- All channels for a well/site stay in the same chunk.
- Missing required channel fails index validation before compute.
- `feature_extraction_tool=deepprofiler` requires Cellpose segmentation.
- Pure CellProfiler workflow does not require Cellpose.
- Active plate batches pass different `params.plates` values to Nextflow.
- `max_chunks` or equivalent subset control limits an Eddie smoke run.

## Performance Review

The largest operational risk is oversubmitting GPU jobs. The plan should add:

- Conservative `maxForks` for GPU labelled processes.
- A separate parameter for chunk-level GPU concurrency.
- Eddie subset mode before full `3723-D-100`.

The current Eddie `queueSize = 100` is acceptable for CPU work, but GPU work needs tighter control.

## Failure Modes

| Failure mode | Covered now? | Required mitigation |
|--------------|--------------|---------------------|
| Active batch not passed to Nextflow, causing duplicate full-batch runs | No | Add batch-specific params generation and dry-run assertion. |
| Missing channel in image set creates partial AI features | No | Fail `BUILD_IMAGESET_INDEX` with a clear error. |
| GPU queue flooded by chunk fan-out | Partial | Add `maxForks` or chunk GPU concurrency param. |
| Cellpose skipped before AI extraction | Partial | Add config and Nextflow branching validation. |
| Illumination calculate output not visible to chunked apply tasks | No | Add integration smoke for `ILLUM_CALCULATE -> ILLUM_APPLY` chunk join. |

No silent critical gap remains if these mitigations are added to the loop plan.

## NOT In Scope

- Per-chunk DataStore staging: rejected because the chosen architecture stages whole scratch-safe plate batches first.
- Full DINO implementation: deferred until model docs and licensing notes are settled.
- Downstream aggregation or hit calling: out of scope because this branch is feature extraction first.
- Reusing SGE command generation: rejected because Nextflow is the new scheduler abstraction.
- Replacing legacy `generate`: out of scope for this phase.

## Parallelization Strategy

Sequential implementation is preferred for the first pass because `nextflow/main.nf`, params generation, and helper script contracts are tightly coupled.

After Loop 250 lands the index/chunk contract, two lanes can run in parallel:

| Lane | Work | Depends on |
|------|------|------------|
| A | Batch-aware CLI params and dry-run assertions | Loop 250 contract |
| B | Chunked Cellpose/feature extraction modules | Loop 250 contract |
| C | Eddie subset validation | A and B |

## Completion Summary

- Scope Challenge: scope accepted with Cellpose conditionality clarified.
- Architecture Review: 3 issues found, all have direct plan changes.
- Code Quality Review: 0 code findings because this is plan-stage only.
- Test Review: 7 required tests identified.
- Performance Review: 1 issue found, GPU throttling.
- NOT in scope: written.
- What already exists: written.
- Failure modes: 5 listed, 0 unresolved critical gaps after planned mitigations.
- Parallelization: sequential first, then 2 implementation lanes after chunk contract.
