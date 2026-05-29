# GPU/MIG Chunk Strategy Design

Date: 2026-05-14

## Goal

Establish an evidence-based Eddie GPU resource policy for cptools2 by testing
whether MIG partitions can safely run parallel Cellpose chunks while heavier
DeepProfiler work remains serialized and, if needed, moves to full GPUs.

This is a validation matrix, but the intent is broader than validation. The
results should inform the operating strategy for future multi-plate,
multi-chunk runs where queue wait, chunk size, failure blast radius, GPU memory,
and stage-specific resource choice all affect total turnaround.

The experiment should compare total turnaround, output completeness, scheduler
acceptance, memory headroom, and failure modes. It should not assume MIG is
equivalent to a full GPU, and it should not change production defaults until
the evidence supports the change.

## Current Evidence

- MIG jobs using `gpu-mig=1` are accepted by Eddie and can run multiple
  Cellpose chunks concurrently.
- Full-GPU jobs using `gpu=1` are accepted by Eddie, but queue wait can vary
  substantially.
- Concurrent DeepProfiler jobs on MIG failed with cuDNN initialization errors.
- Serial DeepProfiler jobs completed on both MIG and full GPU.
- H200 full-GPU validation completed with Cellpose and DeepProfiler when
  DeepProfiler was serialized. A `chunk: 24`, `max_chunks: 5` H200 run produced
  120 Cellpose mask files and 360 DeepProfiler `.npz` feature files.
- H200 Cellpose fan-out across independent one-GPU jobs worked at 8 concurrent
  chunks. Eddie assigned distinct `CUDA_VISIBLE_DEVICES` values, confirming the
  scheduler-managed one-chunk-per-GPU model works for Cellpose on an 8-GPU node.
- H200 DeepProfiler fan-out is not stable with the current DeepProfiler
  TensorFlow container. `CPTOOLS2_FEATURE_MAX_FORKS=8` failed 7 of 8 concurrent
  feature tasks with cuDNN initialization errors, and
  `CPTOOLS2_FEATURE_MAX_FORKS=2` also failed with the same error class.
- A100 compatibility testing should remain in the matrix, but host-group choice
  matters. `gpu@@uoe_GPU_A100_batch` queued while a broader A100 route was
  attempted; scheduler diagnostics showed available-looking A100 capacity may
  still reject the project or queue type. This is a scheduling constraint, not a
  pipeline failure.
- Single-chunk real-cell runs with chunk sizes 24, 48, and 96 completed and
  produced expected feature counts.
- Equal-work multi-chunk runs passed after flattening chunk fan-out and
  serializing DeepProfiler.

## Test Matrix

Use the same approved small real-data subset for all variants. Keep
`max_chunks: 5` fixed so the comparison focuses on resource class and chunk
size.

Because `max_chunks` is fixed, chunk sizes 24, 48, and 96 do not process equal
total work. They test scaling and operating behavior at different image-set
loads. Analysis must normalize by image sets processed per minute and report the
failure blast radius as image sets lost per failed task.

| Variant | Cellpose resource | DeepProfiler resource | DeepProfiler concurrency | Chunk sizes |
| --- | --- | --- | --- | --- |
| MIG-only validation | `gpu-mig=1` | `gpu-mig=1` | serialized | 24, 48, 96 |
| Full-GPU validation | `gpu=1` | `gpu=1` | serialized | 24, 48, 96 |
| Mixed policy candidate | `gpu-mig=1` | `gpu=1` | serialized | best passing size |

## Pre-Execution Requirements

Before submitting the matrix, make the execution controls match the experiment:

- Add process-specific GPU resource controls so Cellpose and DeepProfiler can
  request different resources in the same run, for example
  `CPTOOLS2_SEGMENT_GPU_RESOURCE` and `CPTOOLS2_FEATURE_GPU_RESOURCE`.
- Keep `CPTOOLS2_GPU_RESOURCE` as the shared default for simple MIG-only and
  full-GPU runs.
- Add GPU diagnostics logging to GPU tasks: `hostname`,
  `CUDA_VISIBLE_DEVICES`, `nvidia-smi -L`, and pre/post GPU memory where
  available.
- Use explicit container runtime modes rather than ad hoc command edits:
  `baseline`, `node-local`, `unsquash`, and `scratch-sif`.
- Preserve retry behavior for operational realism, but classify every run as
  `pass_first_attempt`, `pass_after_retry`, or `failed`.
- Ensure the evidence table records expected image-set count, feature-file
  count, mask count, and any zero-cell markers.

## Execution Order

1. Run `chunk: 48`, `max_chunks: 5` with Cellpose and DeepProfiler on MIG.
2. Run the same `chunk: 48`, `max_chunks: 5` config on full GPU.
3. If both pass, repeat with `chunk: 24`.
4. If queue/runtime remains manageable, repeat with `chunk: 96`.
5. If both resource classes pass, run a mixed policy candidate with Cellpose on
   MIG and DeepProfiler on full GPU using the best passing chunk size.

This order starts from the middle chunk size, then explores lower blast radius
and higher throughput options.

## Evidence To Capture

For each run, record:

- Run label, chunk size, `max_chunks`, stage set, and resource policy.
- Queue wait, execution runtime, and total turnaround.
- Image sets processed, image sets per minute, and image sets lost per failed
  task.
- SGE job id, queue, host, hard resources, exit status, and accounting memory.
- Nextflow trace, report, timeline, task status, and first error line.
- Retry attempt count and final classification: `pass_first_attempt`,
  `pass_after_retry`, or `failed`.
- `hostname`, `CUDA_VISIBLE_DEVICES`, and `nvidia-smi -L` from GPU tasks.
- GPU memory evidence where practical, distinguishing it from CPU RSS and
  virtual memory.
- Output counts: masks, locations rows, DeepProfiler feature files, and
  zero-cell markers if present.
- FUSE/squashfuse messages, sandbox extraction fallback, transport endpoint
  errors, bus errors, and teardown symptoms.
- Container runtime mode and cache/tmp placement.

Exact Eddie run roots, dataset identifiers, and job ids should remain in
scratch evidence, not tracked repository files.

## Decision Rules

- MIG may become a production option for Cellpose if parallel chunks pass with
  stable outputs, acceptable queue/runtime, and no recurrent FUSE or memory
  failures.
- DeepProfiler remains serialized unless a dedicated concurrency test later
  proves otherwise. Current H200 evidence shows even two concurrent
  DeepProfiler tasks can fail with cuDNN initialization errors in the current
  container.
- DeepProfiler should prefer full GPU for production-like runs if full GPU gives
  better stability or effective turnaround.
- Chunk size should be chosen from total turnaround and failure blast radius,
  not per-task runtime alone.
- For future multi-plate runs, prefer the policy with the best effective
  turnaround under realistic scheduler packing, not the fastest isolated task.
- Do not infer MIG framebuffer headroom from `maxvmem` or `peak_vmem`; use
  direct GPU evidence where possible.
- Prefer scheduler-managed one-GPU jobs over a custom multi-GPU task wrapper.
  This preserves Nextflow retry semantics, gives per-chunk accounting, and lets
  SGE pack work across A100, H200, L40S, and MIG resources according to
  availability.

## Production Scale Model

The production question is not "which GPU is fastest for one task?" It is:

```text
plate turnaround =
  stage-in time
  + image-set indexing/chunking
  + illumination correction
  + Cellpose queue wait and runtime across chunks
  + DeepProfiler queue wait and serialized runtime across chunks
  + stage-out time
  + retry and recovery cost
```

At multi-plate scale, there are three scheduling layers:

1. **Across plates**: independent plates can run as separate cptools2/Nextflow
   batches, bounded by scratch space and scheduler load.
2. **Across chunks within a plate**: Cellpose can fan out over multiple chunks,
   especially on MIG if memory and FUSE behavior stay stable.
3. **Across feature extraction chunks**: DeepProfiler is intentionally
   serialized for now because concurrent runs failed with cuDNN initialization
   errors.

The likely production shape, if the matrix confirms it, is:

```text
Plate N
  stage in once
  build chunks
  run Cellpose chunks in parallel as independent one-GPU jobs
  run DeepProfiler chunks serially as independent one-GPU jobs
  stage out once
  clean batch work only after successful evidence capture
```

This gives a useful separation of concerns:

- MIG absorbs many smaller Cellpose jobs if queue acceptance is fast and output
  parity holds.
- Full GPU is reserved for heavier DeepProfiler work if it gives better
  stability or throughput.
- H200 can be used for both stages, but only Cellpose should currently use
  multi-job GPU fan-out. DeepProfiler should remain `CPTOOLS2_FEATURE_MAX_FORKS=1`
  until the container/runtime stack is changed and revalidated.
- Chunk size controls failure blast radius and per-task overhead.
- `max_chunks` controls how much of a plate we test, but production runs should
  process all chunks for a plate unless a batch-level limit is deliberately set.

## Scale Questions The Matrix Must Answer

The matrix should produce enough evidence to answer these production questions:

- **How many Cellpose chunks can run in parallel before throughput stops
  improving or failures increase?**
- **Does MIG give better effective Cellpose turnaround than full GPU once queue
  wait is included?**
- **Does DeepProfiler need full GPU for stable production-like throughput, or
  is serialized MIG acceptable for smaller chunks?**
- **Which chunk size balances throughput and recovery cost for real plates?**
- **Does FUSE/container behavior worsen with more concurrent chunks or longer
  task duration?**
- **How much scratch is consumed by a multi-chunk, multi-stage plate before
  cleanup?**
- **What is the expected penalty if one chunk fails and must retry?**

## Production Policy Outputs

After the matrix, write a short policy with these fields:

| Field | Decision to make |
| --- | --- |
| Validation default | Fastest safe GPU class for routine small checks |
| Production Cellpose resource | MIG or full GPU, with concurrency limit |
| Production DeepProfiler resource | MIG or full GPU, serialized unless proven otherwise |
| Default chunk size | Recommended image sets per chunk |
| Max concurrent Cellpose chunks | Resource cap for throughput without instability |
| Retry policy | Whether current retries are acceptable for production |
| Scratch policy | Expected work/cache footprint and cleanup trigger |
| Evidence required before scaling plates | Minimum trace/qacct/output checks |

Do not set a permanent production default until at least one real-data matrix
variant completes with expected feature counts and no unexplained runtime
symptoms.

## Success Criteria

- `chunk: 48`, `max_chunks: 5` completes on both MIG and full GPU, or failures
  are clearly classified.
- At least one chunk-size/resource combination produces complete Cellpose and
  DeepProfiler feature outputs.
- The comparison separates queue wait from execution runtime.
- Matched resource-class runs at the same chunk size publish the expected
  feature-file count, or any difference is explained and classified.
- Results report normalized throughput, image sets per minute, rather than only
  per-run wallclock.
- The final policy states which stages may use MIG, which stages require full
  GPU, and which chunk size is recommended for the next staging/destaging gate.
- The final policy explains how the result scales from one tested subset to
  multi-plate production runs, including concurrency limits and retry cost.

## Out Of Scope

- Broad production runs.
- Forcing one chunk per physical GPU host.
- Committing site-specific paths, run roots, job ids, or dataset identifiers.
- Changing default production resource policy before the matrix is reviewed.
